import { removeGlobals, updateGlobals } from '@platformatic/globals'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { DatabaseWattClient, DatabaseWattProtocolError, DatabaseWattResponseError } from './client'

type SentWattMessage = {
  application: string
  data: Record<string, unknown>
  message: string
  options?: Record<string, unknown>
}

afterEach(() => {
  removeGlobals(['messaging'])
})

describe('Database Watt client transport', () => {
  it('sends typed query requests and validates their responses', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 1, rows: [{ id: 1 }] },
    })
    const client = new DatabaseWattClient()

    const response = await client.query<{ id: number }>({
      destination: createDestination(),
      operationName: 'select-object',
      sql: 'SELECT $1::int AS id',
      values: [1],
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ id: 1 }] })
    expect(sent[0]).toMatchObject({
      application: 'database',
      message: 'database.query',
      data: {
        destination: createDestination(),
        operationName: 'select-object',
        requestId: expect.any(String),
        sql: 'SELECT $1::int AS id',
        values: [1],
      },
      options: { sanitize: false },
    })
  })

  it('surfaces Database Watt error envelopes without PostgreSQL adaptation', async () => {
    installWattMessagingMock({
      'database.query': {
        code: 'POSTGRES_ERROR',
        message: 'duplicate key value violates unique constraint',
        sqlState: '23505',
      },
    })
    const client = new DatabaseWattClient()

    const error = await client
      .query({ destination: createDestination(), sql: 'INSERT' })
      .catch((error: unknown) => error)

    expect(error).toBeInstanceOf(DatabaseWattResponseError)
    expect(error).toMatchObject({
      code: 'POSTGRES_ERROR',
      response: {
        code: 'POSTGRES_ERROR',
        message: 'duplicate key value violates unique constraint',
        sqlState: '23505',
      },
    })
  })

  it('rejects malformed success responses at the transport boundary', async () => {
    installWattMessagingMock({
      'database.query': { rowCount: '1', rows: [] },
    })
    const client = new DatabaseWattClient()

    const error = await client
      .query({ destination: createDestination(), sql: 'SELECT 1' })
      .catch((error: unknown) => error)

    expect(error).toBeInstanceOf(DatabaseWattProtocolError)
    expect(error).toMatchObject({
      code: 'PROTOCOL_ERROR',
      name: 'DatabaseWattProtocolError',
    })
  })

  it.each([
    ['not an array', { rowCount: 0, rows: [], results: {} }],
    ['a malformed result', { rowCount: 0, rows: [], results: [{ rowCount: '1', rows: [] }] }],
  ])('rejects batch responses whose results are %s', async (_, response) => {
    installWattMessagingMock({ 'database.batch': response })
    const client = new DatabaseWattClient()

    const error = await client
      .batch({ destination: createDestination(), statements: [{ sql: 'SELECT 1' }], commit: true })
      .catch((error: unknown) => error)

    expect(error).toBeInstanceOf(DatabaseWattProtocolError)
  })

  it('cancels a lock-bound request with the same request and lock ids', async () => {
    let resolveQuery!: (value: unknown) => void
    const { sent } = installWattMessagingMock({
      'database.lockedQuery': new Promise((resolve) => {
        resolveQuery = resolve
      }),
      'database.cancel': { cancelled: true },
    })
    const client = new DatabaseWattClient()
    const controller = new AbortController()

    const query = client.lockedQuery(
      { lockId: 'lock-a', sql: 'SELECT pg_sleep(10)' },
      { signal: controller.signal }
    )
    controller.abort()

    await expect(query).rejects.toMatchObject({ name: 'AbortError', code: 'ABORT_ERR' })
    resolveQuery({ rowCount: 0, rows: [] })

    expect(sent.map(({ message }) => message)).toEqual(['database.lockedQuery', 'database.cancel'])
    expect(sent[1].data).toMatchObject({
      lockId: 'lock-a',
      requestId: sent[0].data.requestId,
    })
    expect(sent[1].options).toEqual({ sanitize: false })
  })

  it('rolls back a transaction that a batch opened after its caller gave up', async () => {
    let resolveBatch!: (value: unknown) => void
    const { sent } = installWattMessagingMock({
      'database.batch': new Promise((resolve) => {
        resolveBatch = resolve
      }),
      'database.cancel': { cancelled: false },
      'database.rollbackTransaction': { rolledBack: true },
    })
    const client = new DatabaseWattClient()
    const controller = new AbortController()

    const batch = client.batch(
      {
        destination: createDestination(),
        statements: [{ sql: 'SELECT 1' }],
        commit: false,
      },
      { signal: controller.signal }
    )
    controller.abort()

    await expect(batch).rejects.toMatchObject({ name: 'AbortError' })
    resolveBatch({ lockId: 'lock-a', rowCount: 0, rows: [], fields: [] })
    await vi.waitFor(() => expect(sent).toHaveLength(3))

    expect(sent.map(({ message }) => message)).toEqual([
      'database.batch',
      'database.cancel',
      'database.rollbackTransaction',
    ])
    expect(sent[2].data).toMatchObject({ lockId: 'lock-a' })
  })

  it('uses distinct request ids', async () => {
    const { sent } = installWattMessagingMock({
      'database.query': { rowCount: 0, rows: [] },
    })
    const client = new DatabaseWattClient()

    await Promise.all(
      Array.from({ length: 50 }, () =>
        client.query({ destination: createDestination(), sql: 'SELECT 1' })
      )
    )

    expect(new Set(sent.map(({ data }) => data.requestId)).size).toBe(50)
  })
})

function createDestination() {
  return {
    connectionString: 'postgres://tenant-db',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
  }
}

function installWattMessagingMock(responses: Record<string, unknown>): {
  sent: SentWattMessage[]
} {
  const sent: SentWattMessage[] = []

  updateGlobals({
    messaging: {
      handle: vi.fn(),
      notify: vi.fn(),
      getWorkers: vi.fn(),
      getWorkerForAffinity: vi.fn(),
      onWorkersChange: vi.fn(),
      send: vi.fn(
        async (
          application: string,
          message: string,
          data: Record<string, unknown>,
          options?: Record<string, unknown>
        ) => {
          sent.push({ application, data, message, options })
          const response = responses[message]
          return response instanceof Promise ? response : response
        }
      ),
    },
  })

  return { sent }
}
