import { afterEach, describe, expect, it } from 'vitest'
import {
  DatabaseWattPgExecutor,
  getWattPostgresConnection,
  hasWattMessaging,
} from './watt-connection'
import { installWattMessagingMock } from '../../test/utils/watt-messaging'
import type { TenantConnectionOptions } from './pool'

const originalPlatformatic = (globalThis as typeof globalThis & { platformatic?: unknown }).platformatic

afterEach(() => {
  ;(globalThis as typeof globalThis & { platformatic?: unknown }).platformatic = originalPlatformatic
})

describe('Watt PostgreSQL connection adapter', () => {
  it('detects Database Watt messaging availability', () => {
    delete (globalThis as typeof globalThis & { platformatic?: unknown }).platformatic
    expect(hasWattMessaging()).toBe(false)

    installWattMessagingMock()
    expect(hasWattMessaging()).toBe(true)
  })

  it('sends stateless queries through Database Watt messaging', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 1, rows: [{ id: 1 }] },
    })
    const connection = await getWattPostgresConnection(createConnectionOptions())

    const result = await connection.query<{ id: number }>({ text: 'SELECT $1::int as id', values: [1] })

    expect(result.rows).toEqual([{ id: 1 }])
    expect(result.rowCount).toBe(1)
    expect(sent[0]).toMatchObject({
      application: 'database',
      message: 'database.query',
      data: {
        destination: 'tenant-a',
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

    const tx = await connection.transaction({ isolation: 'serializable', readOnly: true })
    const result = await tx.query('SELECT 1')
    await tx.commit()

    expect(result.rows).toEqual([{ ok: true }])
    expect(sent.map((message) => message.message)).toEqual([
      'database.beginTransaction',
      'database.lockedQuery',
      'database.commitTransaction',
    ])
    expect(sent[0].data).toMatchObject({
      destination: 'tenant-a',
      isolationLevel: 'serializable',
      readOnly: true,
    })
    expect(sent[1].data).toMatchObject({ lockId: 'lock-a', sql: 'SELECT 1' })
    expect(tx.isCompleted()).toBe(true)
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

  it('sends explicit cancellation when an AbortSignal fires', async () => {
    let resolveQuery!: (value: unknown) => void
    const { sent } = installWattMessagingMock({
      'database.query':
        new Promise((resolve) => {
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

    expect(sent[0].data.destination).toBe('tenant-a')
    expect(connection.asSuperUser().role).toBe('service_role')
  })

  it('sends master executor queries through Database Watt messaging', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 1, rows: [{ id: 'master' }] },
    })
    const executor = new DatabaseWattPgExecutor('master', () => 'master-operation')

    const result = await executor.query<{ id: string }>('SELECT current_database()')

    expect(result.rows).toEqual([{ id: 'master' }])
    expect(sent[0]).toMatchObject({
      application: 'database',
      message: 'database.query',
      data: {
        destination: 'master',
        operationName: 'master-operation',
        sql: 'SELECT current_database()',
      },
    })
  })

  it('runs master executor transactions through Database Watt messaging', async () => {
    const { sent } = installWattMessagingMock({
      'database.beginTransaction': { lockId: 'master-lock' },
      'database.lockedQuery': { rowCount: 1, rows: [{ ok: true }] },
      'database.commitTransaction': { committed: true },
    })
    const executor = new DatabaseWattPgExecutor('master')

    const tx = await executor.beginTransaction({ isolation: 'serializable', readOnly: true })
    await tx.query('SELECT 1')
    await tx.commit()

    expect(sent.map((message) => message.message)).toEqual([
      'database.beginTransaction',
      'database.lockedQuery',
      'database.commitTransaction',
    ])
    expect(sent[0].data).toMatchObject({
      destination: 'master',
      isolationLevel: 'serializable',
      readOnly: true,
    })
    expect(sent[1].data).toMatchObject({ lockId: 'master-lock', sql: 'SELECT 1' })
  })
})

function createConnectionOptions(): TenantConnectionOptions {
  return {
    dbUrl: '',
    isExternalPool: false,
    isSingleUse: false,
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
