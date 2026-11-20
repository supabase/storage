import EventEmitter from 'node:events'
import { createRequire } from 'node:module'
import { logger, logSchema } from '@internal/monitoring'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { PostgresPubSub } from './postgres'

vi.mock('@internal/monitoring', () => ({
  logger: { info: vi.fn(), warn: vi.fn() },
  logSchema: { info: vi.fn(), warning: vi.fn() },
}))

const require = createRequire(`${process.cwd()}/package.json`)
const pg = require('pg') as { Client: unknown }
const originalClient = pg.Client
const clients: FakeClient[] = []
let failedListens = 0
let failedConnects = 0
let hangingConnect = false
let droppedListens = 0
let listenGate: Promise<void> | undefined

class FakeClient extends EventEmitter {
  readonly queries: string[] = []

  constructor(readonly config: { connectionTimeoutMillis?: number; query_timeout?: number } = {}) {
    super()
    clients.push(this)
  }

  private readonly hangs = hangingConnect

  async connect() {
    if (this.hangs) return new Promise<void>(() => {})
    if (failedConnects > 0) {
      failedConnects--
      return new Promise<void>((_, reject) =>
        setImmediate(() => {
          reject(new Error('connection refused'))
          process.nextTick(() => this.emit('end'))
        })
      )
    }
    queueMicrotask(() => this.emit('connect'))
  }

  async query(sql: string) {
    this.queries.push(sql)
    if (sql.startsWith('LISTEN ') && listenGate) await listenGate
    if (sql.startsWith('LISTEN ') && droppedListens > 0) {
      droppedListens--
      this.emit('end')
      throw new Error('Connection terminated')
    }
    if (sql.startsWith('LISTEN ') && failedListens > 0) {
      failedListens--
      throw new Error('LISTEN failed')
    }
    return {}
  }

  async end() {}
}

beforeEach(() => {
  pg.Client = FakeClient
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
})

afterEach(() => {
  pg.Client = originalClient
  clients.length = 0
  failedListens = 0
  failedConnects = 0
  hangingConnect = false
  droppedListens = 0
  listenGate = undefined
  vi.restoreAllMocks()
})

describe('PostgresPubSub with pg-listen', () => {
  it('logs connection loss, retries failed connect and LISTEN, and delivers on the healthy client', async () => {
    const pubsub = new PostgresPubSub('postgres://test', {
      baseDelayMs: 2,
      maxDelayMs: 4,
      connectionTimeoutMs: 4321,
    })
    const errors: Error[] = []
    const messages: unknown[] = []
    pubsub.on('error', (error) => errors.push(error))
    await pubsub.subscribe('updates', (message) => messages.push(message))
    await pubsub.start()
    expect(clients[0].config.connectionTimeoutMillis).toBeUndefined()
    failedConnects = 1
    failedListens = 1
    clients[0].emit('end')
    await vi.waitFor(
      () =>
        expect(logSchema.warning).toHaveBeenCalledWith(
          logger,
          '[PubSub] Connection lost; reconnecting',
          {
            type: 'pubsub',
          }
        ),
      { interval: 5, timeout: 1000 }
    )

    await vi.waitFor(() => expect(pubsub.isConnected).toBe(true), { interval: 5, timeout: 1000 })
    await new Promise((r) => setTimeout(r, 50))

    expect(clients).toHaveLength(4)
    expect(logSchema.warning).toHaveBeenCalledWith(logger, '[PubSub] Recovery failed; retrying', {
      type: 'pubsub',
      error: expect.objectContaining({ message: 'connection refused' }),
      metadata: expect.any(String),
    })
    const retryWarnings = vi
      .mocked(logSchema.warning)
      .mock.calls.filter(([, m]) => m === '[PubSub] Recovery failed; retrying')
    expect(retryWarnings).toHaveLength(1)
    const meta = JSON.parse((retryWarnings[0][2] as { metadata: string }).metadata)
    expect(meta.attempt).toBe(1)
    expect(typeof meta.downForMs).toBe('number')
    expect(logSchema.info).toHaveBeenCalledWith(logger, '[PubSub] Subscriptions restored', {
      type: 'pubsub',
    })
    expect(clients.slice(1).map((client) => client.config.connectionTimeoutMillis)).toEqual([
      4321, 4321, 4321,
    ])
    expect(clients.map((client) => client.config.query_timeout)).toEqual([
      10_000, 10_000, 10_000, 10_000,
    ])
    expect(pubsub.isConnected).toBe(true)
    expect(errors).toHaveLength(1)
    expect(errors[0].message).toContain('Re-initializing the PostgreSQL notification client')
    clients.at(-1)!.emit('notification', { channel: 'updates', payload: '"restored"' })
    expect(messages).toEqual(['restored'])
    await pubsub.close()
  })

  it('does not create a new client after closing during backoff', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.99)
    const pubsub = new PostgresPubSub('postgres://test', {
      baseDelayMs: 10_000,
      maxDelayMs: 10_000,
    })
    pubsub.on('error', () => {})
    await pubsub.start()
    clients[0].emit('end')
    await vi.waitFor(() => expect(pubsub.isConnected).toBe(false), {
      interval: 5,
      timeout: 1000,
    })
    await pubsub.close()
    expect(clients).toHaveLength(1)
    expect(pubsub.isConnected).toBe(false)
  })

  it('close resolves while a reconnect handshake is pending', async () => {
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 2, maxDelayMs: 2 })
    pubsub.on('error', () => {})
    await pubsub.start()
    hangingConnect = true
    clients[0].emit('end')
    await vi.waitFor(() => expect(clients.length).toBe(2), { interval: 5, timeout: 1000 })

    const closed = vi.fn()
    void pubsub.close().then(closed)
    await vi.waitFor(() => expect(closed).toHaveBeenCalled(), { interval: 5, timeout: 500 })
  })

  it('a drop during LISTEN restore does not tear down the next healthy subscriber', async () => {
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 2, maxDelayMs: 4 })
    const messages: unknown[] = []
    pubsub.on('error', () => {})
    await pubsub.subscribe('updates', (message) => messages.push(message))
    await pubsub.start()
    droppedListens = 1
    clients[0].emit('end')
    await vi.waitFor(() => expect(pubsub.isConnected).toBe(true), { interval: 5, timeout: 1000 })
    await new Promise((r) => setTimeout(r, 50))

    expect(clients).toHaveLength(3)
    expect(pubsub.isConnected).toBe(true)
    clients.at(-1)!.emit('notification', { channel: 'updates', payload: '"ok"' })
    expect(messages).toEqual(['ok'])
    await pubsub.close()
  })

  it('restores the subscriptions current at reconnect time', async () => {
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 30, maxDelayMs: 30 })
    pubsub.on('error', () => {})
    const old = vi.fn()
    const neu = vi.fn()
    await pubsub.subscribe('chan_old', old)
    await pubsub.start()
    clients[0].emit('end')
    await pubsub.unsubscribe('chan_old', old)
    await pubsub.subscribe('chan_new', neu)

    await vi.waitFor(() => expect(pubsub.isConnected).toBe(true), { interval: 5, timeout: 1000 })
    const listens = clients.at(-1)!.queries.filter((q) => q.startsWith('LISTEN '))
    expect(listens.some((q) => q.includes('chan_new'))).toBe(true)
    expect(listens.some((q) => q.includes('chan_old'))).toBe(false)
    clients.at(-1)!.emit('notification', { channel: 'chan_new', payload: '1' })
    expect(neu).toHaveBeenCalledWith(1)
    expect(old).not.toHaveBeenCalled()
    await pubsub.close()
  })

  it('LISTENs a channel subscribed while the restore LISTEN is in flight', async () => {
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 2, maxDelayMs: 2 })
    pubsub.on('error', () => {})
    await pubsub.subscribe('chan_a', () => {})
    await pubsub.start()
    const gate = Promise.withResolvers<void>()
    listenGate = gate.promise
    clients[0].emit('end')
    await vi.waitFor(
      () => {
        expect(clients).toHaveLength(2)
        expect(clients[1].queries.some((q) => q.includes('chan_a'))).toBe(true)
      },
      { interval: 5, timeout: 1000 }
    )
    expect(pubsub.isConnected).toBe(false)

    const received = vi.fn()
    const subscribed = pubsub.subscribe('chan_b', received)
    let resolved = false
    void subscribed.then(() => {
      resolved = true
    })
    await new Promise((r) => setTimeout(r, 20))
    expect(resolved).toBe(false)
    listenGate = undefined
    gate.resolve()
    await subscribed
    expect(pubsub.isConnected).toBe(true)
    expect(clients.at(-1)!.queries.some((q) => q.includes('chan_b'))).toBe(true)
    clients.at(-1)!.emit('notification', { channel: 'chan_b', payload: '"b"' })
    expect(received).toHaveBeenCalledWith('b')
    await pubsub.close()
  })

  it('subscribe during restore rejects when the restore LISTEN fails', async () => {
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 2, maxDelayMs: 2 })
    pubsub.on('error', () => {})
    await pubsub.subscribe('chan_a', () => {})
    await pubsub.start()
    const gate = Promise.withResolvers<void>()
    listenGate = gate.promise
    clients[0].emit('end')
    await vi.waitFor(
      () => {
        expect(clients).toHaveLength(2)
        expect(clients[1].queries.some((q) => q.includes('chan_a'))).toBe(true)
      },
      { interval: 5, timeout: 1000 }
    )

    const subscribed = pubsub.subscribe('chan_b', () => {})
    failedListens = 1
    listenGate = undefined
    gate.resolve()
    await expect(subscribed).rejects.toThrow('LISTEN failed')
    await vi.waitFor(() => expect(pubsub.isConnected).toBe(true), { interval: 5, timeout: 1000 })
    const last = clients.at(-1)!
    expect(last.queries.some((q) => q.includes('chan_a'))).toBe(true)
    expect(last.queries.some((q) => q.includes('chan_b'))).toBe(true)
    await pubsub.close()
  })

  it('throttles recovery warnings to one per minute', async () => {
    let now = 0
    vi.spyOn(Date, 'now').mockImplementation(() => {
      now += 61_000
      return now
    })
    const pubsub = new PostgresPubSub('postgres://test', { baseDelayMs: 2, maxDelayMs: 4 })
    pubsub.on('error', () => {})
    await pubsub.subscribe('updates', () => {})
    await pubsub.start()
    failedConnects = 2
    clients[0].emit('end')
    await vi.waitFor(() => expect(clients.length).toBeGreaterThan(1), {
      interval: 5,
      timeout: 1000,
    })
    await vi.waitFor(() => expect(pubsub.isConnected).toBe(true), { interval: 5, timeout: 1000 })

    const attempts = vi
      .mocked(logSchema.warning)
      .mock.calls.filter(([, m]) => m === '[PubSub] Recovery failed; retrying')
      .map(([, , d]) => JSON.parse((d as { metadata: string }).metadata).attempt)
    expect(attempts).toEqual([1, 2])
    await pubsub.close()
  })

  it('start leaves isConnected false when its LISTEN fails', async () => {
    const pubsub = new PostgresPubSub('postgres://test')
    await pubsub.subscribe('updates', () => {})
    failedListens = 1
    await expect(pubsub.start()).rejects.toThrow('LISTEN failed')
    expect(pubsub.isConnected).toBe(false)
    await pubsub.close()
  })

  it('reports malformed notifications without replacing the subscriber', async () => {
    const pubsub = new PostgresPubSub('postgres://test')
    const errors: Error[] = []
    pubsub.on('error', (error) => errors.push(error))
    await pubsub.start()
    clients[0].emit('notification', { channel: 'updates', payload: 'not json' })
    expect(errors).toHaveLength(1)
    expect(clients).toHaveLength(1)
    expect(pubsub.isConnected).toBe(true)
    await pubsub.close()
  })
})
