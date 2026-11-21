import type { DatabaseExecutor } from './connection'

const TENANT_ENROLLMENT_BATCH_SIZE = 1000

export interface LifecycleTenantClaim {
  tenantId: string
  claimId: string
  claimUntil: Date
}

export class LifecycleTenantStorePg {
  constructor(private readonly database: DatabaseExecutor) {}

  async wakeTenant(tenantId: string, signal?: AbortSignal): Promise<void> {
    await this.database.query(
      {
        text: `
        INSERT INTO lifecycle_tenants (
          tenant_id,
          next_dispatch_at
        ) VALUES (
          $1,
          clock_timestamp()
        )
        ON CONFLICT (tenant_id) DO UPDATE
        SET next_dispatch_at = LEAST(
              lifecycle_tenants.next_dispatch_at,
              EXCLUDED.next_dispatch_at
            )
      `,
        values: [tenantId],
      },
      { signal }
    )
  }

  async enrollAllTenants(): Promise<void> {
    const upperBound = await this.database.query<{ id: string | null }>(
      'SELECT max(id) AS id FROM tenants'
    )
    const lastId = upperBound.rows[0].id
    if (lastId === null) return

    let cursor: string | null = null
    while (true) {
      const batch: { rows: { id: string }[] } = await this.database.query<{ id: string }>({
        text: `
          WITH batch AS MATERIALIZED (
            SELECT id FROM tenants
            WHERE id >= COALESCE($1::text, '') AND id <= $2
              AND ($1::text IS NULL OR id > $1)
            ORDER BY id
            LIMIT $3
          ), enrolled AS (
            INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at)
            SELECT tenants.id, clock_timestamp()
            FROM tenants JOIN batch ON batch.id = tenants.id
            WHERE NOT EXISTS (
              SELECT 1 FROM lifecycle_tenants WHERE tenant_id = tenants.id
            )
            ORDER BY tenants.id
            FOR KEY SHARE OF tenants
            ON CONFLICT (tenant_id) DO NOTHING
          )
          SELECT id FROM batch ORDER BY id DESC LIMIT 1
        `,
        values: [cursor, lastId, TENANT_ENROLLMENT_BATCH_SIZE],
      })
      const next: string | undefined = batch.rows[0]?.id
      if (next === undefined || next === lastId) return
      cursor = next
    }
  }

  async hasObjectVersioningFeature(tenantId: string): Promise<boolean> {
    const result = await this.database.query<{ enabled: boolean }>({
      text: `
        SELECT EXISTS (
          SELECT 1 FROM tenants WHERE tenants.id = $1 AND tenants.feature_object_versioning
        ) AS enabled
      `,
      values: [tenantId],
    })
    return result.rows[0].enabled
  }

  async retireTenant(tenantId: string, claimId: string, deadline?: string): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1
            AND claim_id = $2::uuid
            AND next_dispatch_at = 'infinity'
            AND NOT EXISTS (
              SELECT 1 FROM tenants WHERE tenants.id = $1 AND tenants.feature_object_versioning
            )
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE $3::timestamptz IS NULL OR clock_timestamp() < $3::timestamptz
        )
        DELETE FROM lifecycle_tenants AS target
        USING current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [tenantId, claimId, deadline ?? null],
    })
    return result.rowCount === 1
  }

  async claimDueTenants(limit: number, leaseMs: number): Promise<LifecycleTenantClaim[]> {
    const result = await this.database.query<LifecycleTenantClaim>({
      text: `
        WITH unclaimed AS MATERIALIZED (
          SELECT tenant_id, next_dispatch_at AS due_at
          FROM lifecycle_tenants
          WHERE claim_until IS NULL
            AND next_dispatch_at <= statement_timestamp()
          ORDER BY next_dispatch_at, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        ),
        expired AS MATERIALIZED (
          SELECT tenant_id, claim_until AS due_at
          FROM lifecycle_tenants
          WHERE claim_until < statement_timestamp()
          ORDER BY claim_until, tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        ),
        due AS MATERIALIZED (
          SELECT tenant_id
          FROM (
            SELECT tenant_id, due_at FROM unclaimed
            UNION ALL
            SELECT tenant_id, due_at FROM expired
          ) candidates
          ORDER BY due_at, tenant_id
          LIMIT $1
        )
        -- An expired lease can still have a queued job. Keep its claim id while
        -- backing off repeated redispatches; a successful job clears the failure.
        UPDATE lifecycle_tenants AS target
        SET claim_id = COALESCE(target.claim_id, gen_random_uuid()),
            claim_until = clock_timestamp() + CASE
              WHEN target.claim_until IS NULL THEN $2::bigint * interval '1 millisecond'
              ELSE GREATEST(
                $2::bigint * interval '1 millisecond',
                LEAST(900, 300 * power(2, LEAST(target.failure_count, 7))) * interval '1 second'
              )
            END,
            next_dispatch_at = CASE WHEN target.claim_until IS NULL
              THEN 'infinity'::timestamptz ELSE target.next_dispatch_at END,
            failure_count = CASE WHEN target.claim_until IS NULL
              THEN target.failure_count ELSE target.failure_count + 1 END,
            last_error = CASE WHEN target.claim_until IS NULL THEN target.last_error
              ELSE jsonb_build_object(
                'code', 'tenant_dispatch_lease_expired',
                'message', 'Tenant dispatch lease expired before completion'
              ) END,
            last_dispatched_at = clock_timestamp()
        FROM due
        WHERE target.tenant_id = due.tenant_id
        RETURNING
          target.tenant_id AS "tenantId",
          target.claim_id AS "claimId",
          target.claim_until AS "claimUntil"
      `,
      values: [limit, leaseMs],
    })

    return result.rows
  }

  async completeTenantDispatch(
    tenantId: string,
    claimId: string,
    computedNextDispatchAt: string,
    health?: { maxFailureCount: number; oldestInFlightStartedAt: string | null },
    deadline?: string
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1 AND claim_id = $2::uuid
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE $7::timestamptz IS NULL OR clock_timestamp() < $7::timestamptz
        )
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(next_dispatch_at, $3::timestamptz),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            last_success_at = clock_timestamp(),
            failure_count = 0,
            last_error = NULL,
            max_bucket_failure_count = CASE WHEN $4::boolean THEN $5::integer ELSE max_bucket_failure_count END,
            oldest_in_flight_started_at = CASE WHEN $4::boolean THEN $6::timestamptz ELSE oldest_in_flight_started_at END,
            bucket_status_observed_at = CASE WHEN $4::boolean THEN clock_timestamp() ELSE bucket_status_observed_at END
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [
        tenantId,
        claimId,
        computedNextDispatchAt,
        health !== undefined,
        health?.maxFailureCount ?? null,
        health?.oldestInFlightStartedAt ?? null,
        deadline ?? null,
      ],
    })
    return result.rowCount === 1
  }

  async failTenantDispatch(
    tenantId: string,
    claimId: string,
    error: Record<string, unknown>,
    deadline?: string
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        WITH owned AS (
          SELECT tenant_id FROM lifecycle_tenants
          WHERE tenant_id = $1 AND claim_id = $2::uuid
          FOR UPDATE
        ), current AS (
          SELECT tenant_id FROM owned
          WHERE $4::timestamptz IS NULL OR clock_timestamp() < $4::timestamptz
        )
        UPDATE lifecycle_tenants AS target
        SET next_dispatch_at = LEAST(next_dispatch_at,
              clock_timestamp() + LEAST(21600, 300 * power(2, LEAST(failure_count, 7)))
                * interval '1 second'
            ),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = failure_count + 1,
            last_error = $3::jsonb
        FROM current
        WHERE target.tenant_id = current.tenant_id
      `,
      values: [tenantId, claimId, JSON.stringify(error), deadline ?? null],
    })
    return result.rowCount === 1
  }
}
