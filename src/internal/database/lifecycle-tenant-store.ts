import type { DatabaseExecutor } from './connection'

export interface LifecycleTenantClaim {
  tenantId: string
  claimId: string
}

interface LifecycleTenantClaimRow {
  tenant_id: string
  claim_id: string
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

  async claimDueTenants(limit: number, leaseMs: number): Promise<LifecycleTenantClaim[]> {
    assertPositiveInteger(limit, 'Lifecycle tenant claim limit')
    assertPositiveInteger(leaseMs, 'Lifecycle tenant claim lease')

    const result = await this.database.query<LifecycleTenantClaimRow>({
      text: `
        WITH due AS MATERIALIZED (
          SELECT tenant_id
          FROM lifecycle_tenants
          WHERE (
              next_dispatch_at <= clock_timestamp()
              OR claim_until < clock_timestamp()
            )
            AND (claim_until IS NULL OR claim_until < clock_timestamp())
          ORDER BY
            CASE
              WHEN claim_until < clock_timestamp() THEN claim_until
              ELSE next_dispatch_at
            END,
            tenant_id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        )
        UPDATE lifecycle_tenants AS target
        SET claim_id = gen_random_uuid(),
            claim_until = clock_timestamp() + $2::bigint * interval '1 millisecond',
            next_dispatch_at = 'infinity'::timestamptz,
            last_dispatched_at = clock_timestamp()
        FROM due
        WHERE target.tenant_id = due.tenant_id
        RETURNING
          target.tenant_id,
          target.claim_id
      `,
      values: [limit, leaseMs],
    })

    return result.rows.map((row) => ({
      tenantId: row.tenant_id,
      claimId: row.claim_id,
    }))
  }

  async completeTenantDispatch(
    tenantId: string,
    claimId: string,
    computedNextDispatchAt: string
  ): Promise<boolean> {
    assertTimestamp(computedNextDispatchAt, 'computedNextDispatchAt')
    const result = await this.database.query({
      text: `
        UPDATE lifecycle_tenants
        SET next_dispatch_at = LEAST(next_dispatch_at, $3::timestamptz),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            last_success_at = clock_timestamp(),
            failure_count = 0,
            last_error = NULL
        WHERE tenant_id = $1
          AND claim_id = $2::uuid
      `,
      values: [tenantId, claimId, computedNextDispatchAt],
    })
    return result.rowCount === 1
  }

  async failTenantDispatch(
    tenantId: string,
    claimId: string,
    error: Record<string, unknown>
  ): Promise<boolean> {
    const result = await this.database.query({
      text: `
        UPDATE lifecycle_tenants
        SET next_dispatch_at = LEAST(next_dispatch_at,
              clock_timestamp() + LEAST(21600, 300 * power(2, LEAST(failure_count, 7)))
                * interval '1 second'
            ),
            claim_id = NULL,
            claim_until = NULL,
            last_completed_at = clock_timestamp(),
            failure_count = failure_count + 1,
            last_error = $3::jsonb
        WHERE tenant_id = $1
          AND claim_id = $2::uuid
      `,
      values: [tenantId, claimId, JSON.stringify(error)],
    })
    return result.rowCount === 1
  }
}

function assertPositiveInteger(value: number, label: string) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${label} must be positive`)
}

function assertTimestamp(value: string, label: string) {
  if (Number.isNaN(Date.parse(value))) throw new Error(`${label} must be a timestamp`)
}
