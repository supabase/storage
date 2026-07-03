import { execFileSync } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import tls from 'node:tls'
import {
  attachTlsSessionCapture,
  clearTlsSessionCache,
  getTlsSession,
  getTlsSessionCacheMaxEntries,
  getTlsSessionCacheSize,
  getTlsSessionResumptionClient,
  installTlsSessionInjection,
  observeTlsSessionResumption,
  storeTlsSession,
  TLS_SESSION_CACHE_MAX_AGE_MS,
} from '@internal/database/tls-session-resumption'
import * as metrics from '@internal/monitoring/metrics'
import { vi } from 'vitest'

describe('tls session cache', () => {
  afterEach(() => {
    clearTlsSessionCache()
    vi.restoreAllMocks()
  })

  test('stores and returns the latest session per key', () => {
    const first = Buffer.from('first')
    const second = Buffer.from('second')

    storeTlsSession('host:5432', first)
    expect(getTlsSession('host:5432')).toBe(first)

    storeTlsSession('host:5432', second)
    expect(getTlsSession('host:5432')).toBe(second)
    expect(getTlsSession('other:5432')).toBeUndefined()
  })

  test('expires sessions after the max age', async () => {
    let now = 1
    vi.spyOn(performance, 'now').mockImplementation(() => now)

    storeTlsSession('host:5432', Buffer.from('ticket'))

    now += TLS_SESSION_CACHE_MAX_AGE_MS - 1
    expect(getTlsSession('host:5432')).toBeInstanceOf(Buffer)

    await new Promise((resolve) => setTimeout(resolve, 2))

    now += 2
    expect(getTlsSession('host:5432')).toBeUndefined()
    expect(getTlsSessionCacheSize()).toBe(0)
  })

  test('evicts the oldest entry beyond the size bound', () => {
    const maxEntries = getTlsSessionCacheMaxEntries()
    const session = Buffer.from('ticket')
    for (let i = 0; i <= maxEntries; i++) {
      storeTlsSession(`host-${i}:5432`, session)
    }

    expect(getTlsSessionCacheSize()).toBe(maxEntries)
    expect(getTlsSession('host-0:5432')).toBeUndefined()
    expect(getTlsSession(`host-${maxEntries}:5432`)).toBe(session)
  })

  test('injection getter is enumerable and re-evaluated per Object.assign', () => {
    const ssl: tls.ConnectionOptions = { rejectUnauthorized: false }
    installTlsSessionInjection(ssl, 'host:5432')

    const descriptor = Object.getOwnPropertyDescriptor(ssl, 'session')
    expect(descriptor?.get).toBeInstanceOf(Function)
    expect(descriptor?.enumerable).toBe(true)

    expect(Object.assign({}, ssl).session).toBeUndefined()

    const first = Buffer.from('first')
    storeTlsSession('host:5432', first)
    expect(Object.assign({}, ssl).session).toBe(first)

    const second = Buffer.from('second')
    storeTlsSession('host:5432', second)
    expect(Object.assign({}, ssl).session).toBe(second)
  })

  test('injection install is idempotent', () => {
    const ssl: tls.ConnectionOptions = {}
    installTlsSessionInjection(ssl, 'host:5432')
    installTlsSessionInjection(ssl, 'other:5432')

    storeTlsSession('host:5432', Buffer.from('host-session'))
    storeTlsSession('other:5432', Buffer.from('other-session'))

    expect(Object.assign({}, ssl).session).toEqual(Buffer.from('host-session'))
  })

  test('capture stores every session emitted by the socket', () => {
    const socket = new EventEmitter()
    attachTlsSessionCapture(socket, 'host:5432')

    const first = Buffer.from('first')
    const second = Buffer.from('second')
    socket.emit('session', first)
    expect(getTlsSession('host:5432')).toBe(first)

    socket.emit('session', second)
    expect(getTlsSession('host:5432')).toBe(second)
  })

  test('capture tolerates streams without listener support', () => {
    expect(() => attachTlsSessionCapture(undefined, 'host:5432')).not.toThrow()
    expect(() => attachTlsSessionCapture({}, 'host:5432')).not.toThrow()
    expect(() => attachTlsSessionCapture(null, 'host:5432')).not.toThrow()
  })
})

describe('resumption outcome observability', () => {
  afterEach(() => {
    clearTlsSessionCache()
    vi.restoreAllMocks()
  })

  function fakeTlsSocket(reused: boolean) {
    const socket = new EventEmitter() as EventEmitter & { isSessionReused: () => boolean }
    socket.isSessionReused = () => reused
    return socket
  }

  test("records 'resumed' when the server accepts the offered session", () => {
    const recordSpy = vi.spyOn(metrics, 'recordTlsSessionResumption')
    storeTlsSession('host:5432', Buffer.from('ticket'))

    const socket = fakeTlsSocket(true)
    observeTlsSessionResumption(socket, 'host:5432')
    socket.emit('secureConnect')

    expect(recordSpy).toHaveBeenCalledWith('resumed')
  })

  test("records 'rejected' when a session was offered but the handshake was full", () => {
    const recordSpy = vi.spyOn(metrics, 'recordTlsSessionResumption')
    storeTlsSession('host:5432', Buffer.from('ticket'))

    const socket = fakeTlsSocket(false)
    observeTlsSessionResumption(socket, 'host:5432')
    socket.emit('secureConnect')

    expect(recordSpy).toHaveBeenCalledWith('rejected')
  })

  test("records 'uncached' when no session was available to offer", () => {
    const recordSpy = vi.spyOn(metrics, 'recordTlsSessionResumption')

    const socket = fakeTlsSocket(false)
    observeTlsSessionResumption(socket, 'host:5432')
    socket.emit('secureConnect')

    expect(recordSpy).toHaveBeenCalledWith('uncached')
  })

  test('offer peek does not distort cache request metrics', () => {
    const cacheSpy = vi.spyOn(metrics, 'recordCacheRequest')

    observeTlsSessionResumption(fakeTlsSocket(false), 'host:5432')

    expect(cacheSpy).not.toHaveBeenCalled()
  })

  test('tolerates sockets without resumption support', () => {
    expect(() => observeTlsSessionResumption(new EventEmitter(), 'host:5432')).not.toThrow()
    expect(() => observeTlsSessionResumption(undefined, 'host:5432')).not.toThrow()
    expect(() => observeTlsSessionResumption({}, 'host:5432')).not.toThrow()
  })
})

// A pg upgrade that reshapes event or configuration must fail here
// rather than silently disabling the feature.
describe('TlsSessionResumptionClient pg wiring', () => {
  afterEach(() => {
    clearTlsSessionCache()
    vi.restoreAllMocks()
  })

  test('returns the same class on every call', () => {
    expect(getTlsSessionResumptionClient()).toBe(getTlsSessionResumptionClient())
  })

  test('installs injection on the ssl object and captures sessions on sslconnect', () => {
    const TlsSessionResumptionClient = getTlsSessionResumptionClient()
    const ssl: tls.ConnectionOptions = { rejectUnauthorized: false }
    const client = new TlsSessionResumptionClient({
      host: '1.2.3.4',
      port: 5433,
      user: 'user',
      password: 'password',
      database: 'db',
      ssl,
    })

    const descriptor = Object.getOwnPropertyDescriptor(ssl, 'session')
    expect(descriptor?.get).toBeInstanceOf(Function)

    const internals = client as unknown as {
      connection: EventEmitter & { stream: unknown }
    }
    expect(internals.connection).toBeDefined()
    expect(internals.connection.listenerCount('sslconnect')).toBe(1)

    const recordSpy = vi.spyOn(metrics, 'recordTlsSessionResumption')
    const stream = new EventEmitter() as EventEmitter & { isSessionReused: () => boolean }
    stream.isSessionReused = () => false
    internals.connection.stream = stream
    internals.connection.emit('sslconnect')

    const session = Buffer.from('ticket')
    stream.emit('session', session)
    expect(getTlsSession('1.2.3.4:5433')).toBe(session)

    expect(Object.assign({}, ssl).session).toBe(session)

    stream.emit('secureConnect')
    expect(recordSpy).toHaveBeenCalledWith('uncached')
  })

  test('does nothing without object-form ssl settings', () => {
    const TlsSessionResumptionClient = getTlsSessionResumptionClient()
    const client = new TlsSessionResumptionClient({
      host: 'db.example.com',
      port: 5432,
      user: 'user',
      password: 'password',
      database: 'db',
    })

    const internals = client as unknown as { connection: EventEmitter }
    expect(internals.connection.listenerCount('sslconnect')).toBe(0)
  })
})

const opensslAvailable = (() => {
  try {
    execFileSync('openssl', ['version'], { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
})()

describe.runIf(opensslAvailable)('TLS session resumption against a real TLS server', () => {
  let certDir: string
  let serverKey: Buffer
  let serverCert: Buffer

  beforeAll(() => {
    certDir = mkdtempSync(join(tmpdir(), 'tls-resume-'))
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'ec',
        '-pkeyopt',
        'ec_paramgen_curve:prime256v1',
        '-nodes',
        '-keyout',
        join(certDir, 'key.pem'),
        '-out',
        join(certDir, 'cert.pem'),
        '-days',
        '2',
        '-subj',
        '/CN=localhost',
      ],
      { stdio: 'ignore' }
    )
    serverKey = readFileSync(join(certDir, 'key.pem'))
    serverCert = readFileSync(join(certDir, 'cert.pem'))
  })

  afterAll(() => {
    rmSync(certDir, { recursive: true, force: true })
  })

  afterEach(() => {
    clearTlsSessionCache()
  })

  test.each([['TLSv1.2'], ['TLSv1.3']])('resumes a session over %s', async (version) => {
    const tlsVersion = version as tls.SecureVersion
    const server = tls.createServer({
      key: serverKey,
      cert: serverCert,
      minVersion: tlsVersion,
      maxVersion: tlsVersion,
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as AddressInfo).port
    const cacheKey = `127.0.0.1:${port}`

    try {
      const first = tls.connect({ host: '127.0.0.1', port, rejectUnauthorized: false })
      attachTlsSessionCapture(first, cacheKey)
      const sessionArrived = new Promise<void>((resolve, reject) => {
        first.once('session', () => resolve())
        first.once('error', reject)
      })
      await new Promise<void>((resolve, reject) => {
        first.once('secureConnect', resolve)
        first.once('error', reject)
      })
      expect(first.isSessionReused()).toBe(false)
      await sessionArrived
      first.destroy()

      expect(getTlsSession(cacheKey)).toBeInstanceOf(Buffer)

      const ssl: tls.ConnectionOptions = { rejectUnauthorized: false }
      installTlsSessionInjection(ssl, cacheKey)
      const options: tls.ConnectionOptions = { host: '127.0.0.1', port }
      Object.assign(options, ssl)

      const second = tls.connect(options)
      await new Promise<void>((resolve, reject) => {
        second.once('secureConnect', resolve)
        second.once('error', reject)
      })
      const reused = second.isSessionReused()
      second.destroy()

      expect(reused).toBe(true)
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
