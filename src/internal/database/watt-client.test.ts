import { removeGlobals, updateGlobals } from '@platformatic/globals'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  DatabaseWattClient,
  DatabaseWattProtocolError,
  DatabaseWattResponseError,
} from './watt-client'

type SentWattMessage = {
  application: string
  data: Record<string, unknown>
  message: string
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
      destination: 'tenant-a',
      operationName: 'select-object',
      sql: 'SELECT $1::int AS id',
      values: [1],
    })

    expect(response).toEqual({ rowCount: 1, rows: [{ id: 1 }] })
    expect(sent[0]).toMatchObject({
      application: 'database',
      message: 'database.query',
      data: {
        destination: 'tenant-a',
        operationName: 'select-object',
        requestId: expect.any(String),
        sql: 'SELECT $1::int AS id',
        values: [1],
      },
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
      .query({ destination: 'tenant-a', sql: 'INSERT' })
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
      .query({ destination: 'tenant-a', sql: 'SELECT 1' })
      .catch((error: unknown) => error)

    expect(error).toBeInstanceOf(DatabaseWattProtocolError)
    expect(error).toMatchObject({
      code: 'PROTOCOL_ERROR',
      name: 'DatabaseWattProtocolError',
    })
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
  })
})

function installWattMessagingMock(responses: Record<string, unknown>): {
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
