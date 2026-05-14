import { getConfig } from '../../../config'
import { PgTransaction, PgTransactionalExecutor } from '../../database/pg-connection'
import { logger, logSchema } from '../../monitoring'
import { JWKSManagerStore, JWKStoreItem, PaginatedTenantItem } from './store'

const { multitenantDatabaseQueryTimeout } = getConfig()

export class JWKSManagerStorePg implements JWKSManagerStore<PgTransaction> {
  constructor(private db: PgTransactionalExecutor) {}

  async transaction<T>(callback: (trx: PgTransaction) => Promise<T>): Promise<T> {
    const trx = await this.db.beginTransaction()

    try {
      const result = await callback(trx)
      await trx.commit()
      return result
    } catch (e) {
      try {
        await trx.rollback()
      } catch (rollbackError) {
        logSchema.warning(logger, '[JWKSManagerStorePg] Failed to rollback transaction', {
          type: 'db',
          error: rollbackError,
          metadata: JSON.stringify({ originalError: String(e) }),
        })
      }
      throw e
    }
  }

  async insert(
    tenantId: string,
    encryptedJwk: string,
    kind: string,
    idempotent = false,
    trx?: PgTransaction
  ): Promise<string> {
    const db = trx || this.db
    const insertResult = await db.query<{ id: string }>(
      {
        text: `
          INSERT INTO tenants_jwks (
            tenant_id,
            content,
            kind,
            active
          )
          VALUES ($1, $2, $3, true)
          ${idempotent ? 'ON CONFLICT DO NOTHING' : ''}
          RETURNING id
        `,
        values: [tenantId, encryptedJwk, kind],
      },
      { signal: AbortSignal.timeout(multitenantDatabaseQueryTimeout) }
    )

    if (insertResult.rows.length > 0) {
      return insertResult.rows[0].id
    }

    if (!idempotent) {
      throw new Error('failed to insert jwk')
    }

    const result = await db.query<{ id: string }>(
      {
        text: `
          SELECT id
          FROM tenants_jwks
          WHERE tenant_id = $1
            AND kind = $2
            AND active = true
          LIMIT 1
        `,
        values: [tenantId, kind],
      },
      { signal: AbortSignal.timeout(multitenantDatabaseQueryTimeout) }
    )

    if (!result.rows[0]?.id) {
      throw new Error('failed to find existing jwk on idempotent insert')
    }

    return result.rows[0].id
  }

  async toggleActive(
    tenantId: string,
    id: string,
    newState: boolean,
    trx?: PgTransaction
  ): Promise<boolean> {
    const db = trx || this.db
    const result = await db.query(
      {
        text: `
          UPDATE tenants_jwks
          SET active = $1
          WHERE id = $2
            AND tenant_id = $3
            AND active = $4
        `,
        values: [newState, id, tenantId, !newState],
      },
      { signal: AbortSignal.timeout(multitenantDatabaseQueryTimeout) }
    )

    return Boolean(result.rowCount && result.rowCount > 0)
  }

  async listActive(tenantId: string, kind?: string, trx?: PgTransaction): Promise<JWKStoreItem[]> {
    const db = trx || this.db
    const result = await db.query<JWKStoreItem>(
      {
        text: `
          SELECT id, kind, content
          FROM tenants_jwks
          WHERE tenant_id = $1
            AND active = true
            AND ($2::text IS NULL OR kind = $2)
        `,
        values: [tenantId, kind || null],
      },
      { signal: AbortSignal.timeout(multitenantDatabaseQueryTimeout) }
    )

    return result.rows
  }

  async listTenantsWithoutKindPaginated(
    kind: string,
    batchSize: number,
    lastCursor = 0
  ): Promise<PaginatedTenantItem[]> {
    const result = await this.db.query<PaginatedTenantItem>(
      {
        text: `
          SELECT id, cursor_id
          FROM tenants
          WHERE cursor_id > $1
            AND NOT EXISTS (
              SELECT 1
              FROM tenants_jwks
              WHERE tenants_jwks.tenant_id = tenants.id
                AND tenants_jwks.kind = $2
                AND tenants_jwks.active = true
            )
          ORDER BY cursor_id ASC
          LIMIT $3
        `,
        values: [lastCursor, kind, batchSize],
      },
      { signal: AbortSignal.timeout(multitenantDatabaseQueryTimeout) }
    )

    return result.rows
  }
}
