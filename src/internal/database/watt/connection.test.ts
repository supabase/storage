import { removeGlobals, updateGlobals } from '@platformatic/globals'
import { DatabaseError } from 'pg'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { DatabaseExecutor } from '../connection'
import { searchPath, type TenantConnectionOptions } from '../pool'
import type { DatabaseWattTransport } from './client'
import { getWattPostgresConnection, WattPgExecutor } from './connection'

type SentWattMessage = {
  application: string
  data: Record<string, unknown>
  message: string
}

function installWattMessagingMock(responses: Record<string, unknown> = {}): {
  sent: SentWattMessage[]
} {
  const sent: SentWattMessage[] = []

  updateGlobals({
    messaging: {
      handle: vi.fn(),
      notify: vi.fn(),
      send: vi.fn(async (application: string, message: string, data: Record<string, unknown>) => {
        sent.push({ application, data, message })
        const response = responses[message]
        return response instanceof Promise ? response : response
      }),
    },
  })

  return { sent }
}

afterEach(() => {
  removeGlobals(['messaging'])
})

describe('Watt PostgreSQL connection adapter', () => {
  it('adapts an injected Watt transport without Platformatic messaging', async () => {
    const transport: DatabaseWattTransport = {
      query: vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ id: 1 }] }),
      acquire: vi.fn(),
      lockedQuery: vi.fn(),
      release: vi.fn(),
      beginTransaction: vi.fn(),
      commitTransaction: vi.fn(),
      rollbackTransaction: vi.fn(),
    }
    const executor = new WattPgExecutor(createPoolTarget(), () => 'operation-a', transport)

    const result = await executor.query<{ id: number }>('SELECT $1::int AS id', [1])

    expect(result).toMatchObject({ rowCount: 1, rows: [{ id: 1 }] })
    expect(transport.query).toHaveBeenCalledWith(
      {
        destination: createPoolTarget(),
        operationName: 'operation-a',
        sql: 'SELECT $1::int AS id',
        values: [1],
      },
      { signal: undefined }
    )
  })

  it('sends stateless queries through Database Watt messaging', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 1, rows: [{ id: 1 }] },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    const result = await connection.query<{ id: number }>({
      text: 'SELECT $1::int as id',
      values: [1],
    })

    expect(result.rows).toEqual([{ id: 1 }])
    expect(result.rowCount).toBe(1)
    expect(sent[0]).toMatchObject({
      application: 'database',
      message: 'database.query',
      data: {
        destination: createPoolTarget(),
        operationName: 'operation-a',
        sql: 'SELECT $1::int as id',
        values: [1],
      },
    })
    expect(sent[0].data.requestId).toEqual(expect.any(String))
  })

  it('runs transaction lifecycle through lock-bound messages', async () => {
    const { sent } = installWattMessagingMock({
      'database.beginTransaction': { lockId: 'lock-a' },
      'database.lockedQuery': { rowCount: 1, rows: [{ ok: true }] },
      'database.commitTransaction': { committed: true },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    const tx = await connection.transaction({
      isolation: 'serializable',
      readOnly: true,
      timeout: 5_000,
    })
    const result = await tx.query('SELECT 1')
    await tx.commit()

    expect(result.rows).toEqual([{ ok: true }])
    expect(sent.map((message) => message.message)).toEqual([
      'database.beginTransaction',
      'database.lockedQuery',
      'database.lockedQuery',
      'database.commitTransaction',
    ])
    expect(sent[0].data).toMatchObject({
      destination: createPoolTarget(),
      isolationLevel: 'SERIALIZABLE',
      readOnly: true,
    })
    // Without setScope the deferred statement timeout is applied before the first query.
    expect(sent[1].data).toMatchObject({
      lockId: 'lock-a',
      sql: "SELECT set_config('statement_timeout', $1, true)",
      values: ['5000ms'],
    })
    expect(sent[2].data).toMatchObject({ lockId: 'lock-a', sql: 'SELECT 1' })
    expect(tx.isCompleted()).toBe(true)
  })

  it('folds transaction settings into the scope statement', async () => {
    const { sent } = installWattMessagingMock({
      'database.beginTransaction': { lockId: 'lock-a' },
      'database.lockedQuery': { rowCount: 1, rows: [{ ok: true }] },
    })
    const connection = await getWattPostgresConnection({
      ...createConnectionOptions(),
      isExternalPool: true,
    })

    const tx = await connection.transaction({ timeout: 5_000 })
    await connection.setScope(tx)
    await tx.query('SELECT 1')

    expect(sent.map((message) => message.message)).toEqual([
      'database.beginTransaction',
      'database.lockedQuery',
      'database.lockedQuery',
    ])
    expect(sent[1].data.sql).toContain("set_config('statement_timeout', $10, true)")
    expect(sent[1].data.sql).toContain("set_config('search_path', $11, true)")
    expect((sent[1].data.values as unknown[]).slice(9)).toEqual(['5000ms', searchPath.join(',')])
    expect(sent[2].data).toMatchObject({ sql: 'SELECT 1' })
  })

  it('sends unknown isolation levels as a plain BEGIN like the direct path', async () => {
    const { sent } = installWattMessagingMock({
      'database.beginTransaction': { lockId: 'lock-a' },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    await connection.beginTransaction({ isolation: 'read uncommitted' })

    expect(sent[0].data.isolationLevel).toBeUndefined()
  })

  it('splits the tenant connection budget across the cluster', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 0, rows: [] },
    })
    const connection = await getWattPostgresConnection({
      ...createConnectionOptions(),
      clusterSize: 3,
      maxConnections: 10,
    })

    await connection.query('SELECT 1')

    expect(sent[0].data.destination).toMatchObject({ maxConnections: 4 })
  })

  it('rolls back transactions through Database Watt messaging', async () => {
    const { sent } = installWattMessagingMock({
      'database.beginTransaction': { lockId: 'lock-a' },
      'database.rollbackTransaction': { rolledBack: true },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    const tx = await connection.transaction()
    await tx.rollback()

    expect(sent.map((message) => message.message)).toEqual([
      'database.beginTransaction',
      'database.rollbackTransaction',
    ])
    expect(tx.isCompleted()).toBe(true)
  })

  it('maps PostgreSQL error responses back to pg DatabaseError', async () => {
    installWattMessagingMock({
      'database.query': {
        code: 'POSTGRES_ERROR',
        message: 'duplicate key value violates unique constraint',
        sqlState: '23505',
      },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    await expect(connection.query('INSERT')).rejects.toMatchObject({
      code: '23505',
      message: 'duplicate key value violates unique constraint',
    })
  })

  it('restores PostgreSQL error fields used by Storage error mapping', async () => {
    installWattMessagingMock({
      'database.query': {
        code: 'POSTGRES_ERROR',
        message: 'update or delete violates foreign key constraint',
        sqlState: '23503',
        postgres: {
          constraint: 's3_multipart_uploads_bucket_id_fkey',
          detail: 'Key (id)=(bucket) is still referenced from table "s3_multipart_uploads".',
        },
      },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    const error = await connection.query('DELETE').catch((e: unknown) => e)

    expect(error).toBeInstanceOf(DatabaseError)
    expect(error).toMatchObject({
      code: '23503',
      constraint: 's3_multipart_uploads_bucket_id_fkey',
      detail: 'Key (id)=(bucket) is still referenced from table "s3_multipart_uploads".',
    })
  })

  it('completes a transaction whose connection the Database application discarded', async () => {
    installWattMessagingMock({
      'database.beginTransaction': { lockId: 'lock-a' },
      'database.lockedQuery': {
        code: 'POSTGRES_ERROR',
        message: 'terminating connection due to administrator command',
        sqlState: '57P01',
        connectionDiscarded: true,
      },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())
    const tx = await connection.beginTransaction()

    await expect(tx.query('SELECT 1')).rejects.toMatchObject({ code: '57P01' })
    expect(tx.isCompleted()).toBe(true)
  })

  it('maps Platformatic messaging timeouts to DatabaseTimeout and cancels the request', async () => {
    const sent: SentWattMessage[] = []
    updateGlobals({
      messaging: {
        handle: vi.fn(),
        notify: vi.fn(),
        send: vi.fn(async (application: string, message: string, data: Record<string, unknown>) => {
          sent.push({ application, data, message })
          if (message === 'database.cancel') {
            return { cancelled: true }
          }
          const error = new Error(
            'Cannot send a message to application "database": Timeout while waiting for a response.'
          ) as Error & { code: string }
          error.code = 'PLT_RUNTIME_MESSAGING_ERROR'
          throw error
        }),
      },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    await expect(connection.query('SELECT pg_sleep(60)')).rejects.toMatchObject({
      code: 'DatabaseTimeout',
      httpStatusCode: 544,
    })
    expect(sent.map((message) => message.message)).toEqual(['database.query', 'database.cancel'])
    expect(sent[1].data.requestId).toBe(sent[0].data.requestId)
  })

  it.each(['ACQUIRE_TIMEOUT', 'BUSY', 'CONNECTION_TIMEOUT'])(
    'maps %s to the DatabaseTimeout error a direct pool returns',
    async (code) => {
      installWattMessagingMock({
        'database.query': {
          code,
          message: 'Timed out acquiring database connection',
        },
      })
      const connection = await getWattPostgresConnection(createConnectionOptions())

      const error = (await connection.query('SELECT 1').catch((e: unknown) => e)) as {
        getOriginalError(): unknown
      }

      expect(error).toMatchObject({ code: 'DatabaseTimeout', httpStatusCode: 544 })
      expect(error.getOriginalError()).toMatchObject({ code })
    }
  )

  it('maps server timeouts to PostgreSQL query-canceled SQLSTATE', async () => {
    installWattMessagingMock({
      'database.query': {
        code: 'SERVER_TIMEOUT',
        message: 'canceling statement due to statement timeout',
      },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    await expect(connection.query('SELECT pg_sleep(10)')).rejects.toMatchObject({
      code: '57014',
      message: 'canceling statement due to statement timeout',
    })
  })

  it('sends explicit cancellation when an AbortSignal fires', async () => {
    let resolveQuery!: (value: unknown) => void
    const { sent } = installWattMessagingMock({
      'database.query': new Promise((resolve) => {
        resolveQuery = resolve
      }),
      'database.cancel': { cancelled: true },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())
    const controller = new AbortController()

    const query = connection.query('SELECT pg_sleep(10)', { signal: controller.signal })
    controller.abort()

    await expect(query).rejects.toMatchObject({ name: 'AbortError', code: 'ABORT_ERR' })
    resolveQuery({ rowCount: 0, rows: [] })

    expect(sent.map((message) => message.message)).toEqual(['database.query', 'database.cancel'])
    expect(sent[1].data.requestId).toBe(sent[0].data.requestId)
  })

  it('creates superuser connections with service role credentials', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 1, rows: [] },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    await connection.asSuperUser().query('SELECT 1')

    expect(sent[0].data.destination).toEqual(createPoolTarget())
    expect(connection.asSuperUser().role).toBe('service_role')
  })

  it('uses the shared PostgreSQL scope statement', async () => {
    const connection = await getWattPostgresConnection({
      ...createConnectionOptions(),
      headers: { 'x-client-info': 'test' },
      method: 'POST',
      path: '/object/bucket/name',
    })
    const query = vi.fn().mockResolvedValue({ rowCount: 0, rows: [] })

    await connection.setScope({ query } as unknown as DatabaseExecutor)

    expect(query).toHaveBeenCalledTimes(1)
    const statement = query.mock.calls[0][0]
    expect(statement.text).toContain("set_config('role', $1, true)")
    // Direct (non-pooler) destinations get search_path as a connection option.
    expect(statement.text).not.toContain('search_path')
    expect(statement.values).toEqual([
      'authenticated',
      'authenticated',
      'user-jwt',
      '',
      '{"role":"authenticated"}',
      '{"x-client-info":"test"}',
      'POST',
      '/object/bucket/name',
      'operation-a',
    ])
  })
})

function createConnectionOptions(): TenantConnectionOptions {
  return {
    dbUrl: 'postgres://tenant-db',
    isExternalPool: false,
    maxConnections: 10,
    operation: () => 'operation-a',
    superUser: {
      jwt: 'service-jwt',
      payload: { role: 'service_role' },
    },
    tenantId: 'tenant-a',
    user: {
      jwt: 'user-jwt',
      payload: { role: 'authenticated' },
    },
  }
}

function createPoolTarget() {
  return {
    connectionString: 'postgres://tenant-db',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
  }
}
