import { JWKSManagerStorePg } from '@internal/auth/jwks'
import { PgPoolExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { Pool } from 'pg'
import { getConfig } from '../config'
import { runMultitenantMigrations } from '../internal/database/migrations'

const { databaseApplicationName, multitenantDatabaseUrl } = getConfig()

const tenantId = 'pg-store-jwks'
const tenantWithoutKindId = 'pg-store-jwks-missing-kind'

describe('JWKSManagerStorePg', () => {
  let pool: Pool
  let executor: PgPoolExecutor
  let store: JWKSManagerStorePg

  beforeAll(async () => {
    await runMultitenantMigrations()

    pool = new Pool({
      connectionString: multitenantDatabaseUrl,
      application_name: databaseApplicationName,
      max: 2,
      min: 0,
    })
    executor = new PgPoolExecutor(pool)
    store = new JWKSManagerStorePg(executor)
  })

  beforeEach(async () => {
    await executor.query({
      text: `
        INSERT INTO tenants (
          id,
          anon_key,
          database_url,
          jwt_secret,
          service_key
        )
        VALUES
          ($1, 'anon', 'postgres://tenant', 'jwt-secret', 'service-key'),
          ($2, 'anon', 'postgres://tenant', 'jwt-secret', 'service-key')
        ON CONFLICT (id) DO NOTHING
      `,
      values: [tenantId, tenantWithoutKindId],
    })
  })

  afterEach(async () => {
    await executor.query({
      text: 'DELETE FROM tenants WHERE id = ANY($1::text[])',
      values: [[tenantId, tenantWithoutKindId]],
    })
  })

  afterAll(async () => {
    await pool.end()
  })

  it('implements insert, listActive, and toggleActive', async () => {
    const id = await store.insert(tenantId, 'encrypted-content', 'custom-kind')

    await expect(store.listActive(tenantId)).resolves.toEqual([
      {
        id,
        kind: 'custom-kind',
        content: 'encrypted-content',
      },
    ])

    await expect(store.listActive(tenantId, 'other-kind')).resolves.toEqual([])
    await expect(store.toggleActive(tenantId, id, false)).resolves.toBe(true)
    await expect(store.toggleActive(tenantId, id, false)).resolves.toBe(false)
    await expect(store.listActive(tenantId)).resolves.toEqual([])
    await expect(store.toggleActive(tenantId, id, true)).resolves.toBe(true)
    await expect(store.listActive(tenantId, 'custom-kind')).resolves.toHaveLength(1)
  })

  it('returns the existing row for idempotent active url signing key inserts', async () => {
    const firstId = await store.insert(
      tenantId,
      'first-encrypted-content',
      'storage-url-signing-key',
      true
    )
    const secondId = await store.insert(
      tenantId,
      'second-encrypted-content',
      'storage-url-signing-key',
      true
    )

    expect(secondId).toBe(firstId)
    await expect(store.listActive(tenantId, 'storage-url-signing-key')).resolves.toEqual([
      {
        id: firstId,
        kind: 'storage-url-signing-key',
        content: 'first-encrypted-content',
      },
    ])
  })

  it('rolls back transaction work when the callback fails', async () => {
    await expect(
      store.transaction(async (trx) => {
        await store.insert(tenantId, 'transaction-content', 'transaction-kind', false, trx)
        throw new Error('rollback requested')
      })
    ).rejects.toThrow('rollback requested')

    await expect(store.listActive(tenantId, 'transaction-kind')).resolves.toEqual([])
  })

  it('preserves transaction callback errors when rollback fails', async () => {
    const originalError = new Error('transaction failed')
    const rollbackError = new Error('rollback failed')
    const trx = {
      commit: vi.fn().mockResolvedValue(undefined),
      rollback: vi.fn().mockRejectedValue(rollbackError),
    }
    const storage = new JWKSManagerStorePg({
      beginTransaction: vi.fn().mockResolvedValue(trx),
    } as never)
    const logSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)

    try {
      await expect(
        storage.transaction(async () => {
          throw originalError
        })
      ).rejects.toBe(originalError)

      expect(trx.rollback).toHaveBeenCalledTimes(1)
      expect(logSpy).toHaveBeenCalledWith(
        logger,
        '[JWKSManagerStorePg] Failed to rollback transaction',
        expect.objectContaining({
          type: 'db',
          error: rollbackError,
        })
      )
    } finally {
      logSpy.mockRestore()
    }
  })

  it('commits transaction work when the callback succeeds', async () => {
    const id = await store.transaction((trx) => {
      return store.insert(tenantId, 'committed-content', 'committed-kind', false, trx)
    })

    await expect(store.listActive(tenantId, 'committed-kind')).resolves.toEqual([
      {
        id,
        kind: 'committed-kind',
        content: 'committed-content',
      },
    ])
  })

  it('lists tenants missing a specific active jwk kind', async () => {
    await store.insert(tenantId, 'present-content', 'present-kind')

    const tenants = await store.listTenantsWithoutKindPaginated('present-kind', 100)
    const tenantIds = tenants.map((tenant) => tenant.id)

    expect(tenantIds).toContain(tenantWithoutKindId)
    expect(tenantIds).not.toContain(tenantId)
  })
})
