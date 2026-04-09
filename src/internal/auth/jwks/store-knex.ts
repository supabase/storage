import { Knex } from 'knex'
import { getConfig } from '../../../config'
import { JWKSManagerStore, JWKStoreItem, PaginatedTenantItem } from './store'

const { multitenantDatabaseQueryTimeout } = getConfig()

export class JWKSManagerStoreKnex implements JWKSManagerStore<Knex.Transaction> {
  constructor(private knex: Knex) {}

  async transaction<T>(callback: (trx: Knex.Transaction) => Promise<T>): Promise<T> {
    return this.knex.transaction(callback)
  }

  async insert(
    tenant_id: string,
    content: string,
    kind: string,
    idempotent = false,
    trx?: Knex.Transaction
  ): Promise<string> {
    const db = trx || this.knex
    const insertQuery = db('tenants_jwks')
      .insert({
        tenant_id,
        content,
        kind,
        active: true,
      })
      .returning('id')

    if (idempotent) {
      insertQuery.onConflict().ignore()
    }

    const insertResult = await insertQuery

    if (insertResult.length > 0) {
      // row inserted successfully
      return insertResult[0].id
    } else if (!idempotent) {
      throw new Error('failed to insert jwk')
    } else {
      // if insert failed due to the unique constraint return the conflicting existing entry instead
      const result = await db('tenants_jwks')
        .select('id')
        .where({ tenant_id, kind, active: true })
        .first<{ id: string }>()

      if (!result?.id) {
        throw new Error('failed to find existing jwk on idempotent insert')
      }
      return result.id
    }
  }

  async toggleActive(
    tenantId: string,
    id: string,
    newState: boolean,
    trx?: Knex.Transaction
  ): Promise<boolean> {
    const db = trx || this.knex
    const updated = await db
      .table('tenants_jwks')
      .where('id', id)
      .where('tenant_id', tenantId)
      .where('active', !newState)
      .update({ active: newState })
      .abortOnSignal(AbortSignal.timeout(multitenantDatabaseQueryTimeout))
    return updated > 0
  }

  listActive(tenantId: string, kind?: string): Promise<JWKStoreItem[]> {
    const query = this.knex
      .table<JWKStoreItem>('tenants_jwks')
      .select('id', 'kind', 'content')
      .where('tenant_id', tenantId)
      .where('active', true)

    if (kind) {
      query.where('kind', kind)
    }

    return query.abortOnSignal(AbortSignal.timeout(multitenantDatabaseQueryTimeout))
  }

  async listTenantsWithoutKindPaginated(
    kind: string,
    batchSize: number,
    lastCursor = 0
  ): Promise<PaginatedTenantItem[]> {
    return this.knex('tenants')
      .select<PaginatedTenantItem[]>('id', 'cursor_id')
      .where('cursor_id', '>', lastCursor)
      .whereNotExists(function () {
        this.select(1)
          .from('tenants_jwks')
          .whereRaw('tenants_jwks.tenant_id = tenants.id')
          .andWhere('tenants_jwks.kind', kind)
          .andWhere('tenants_jwks.active', true)
      })
      .orderBy('cursor_id', 'asc')
      .limit(batchSize)
      .abortOnSignal(AbortSignal.timeout(multitenantDatabaseQueryTimeout))
  }
}
