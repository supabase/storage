import {
  createLruCache,
  DisposableCache,
  LruCacheSetOptions,
  TLS_SESSION_CACHE_NAME,
} from '@internal/cache'
import { recordTlsSessionResumption } from '@internal/monitoring/metrics'
import { Client } from 'pg'
import type { ConnectionOptions } from 'tls'
import { TENANT_CONFIG_CACHE_MAX_ITEMS } from './tenant'

/**
 * Client-side TLS session cache for tenant DB connections.
 *
 * Node skips the per-handshake certificate extraction and identity verification work
 * only when a session is resumed and resumed handshakes are also cheaper on the wire.
 * pg implements no client session cache, so every reconnect pays the full handshake.
 * Pass a getter in ssl options of the pool which is reevaluated on every physical
 * connection and capture the session from the TLS socket. There can be multiple session
 * tickets per host but last one wins. Finally, a stale or rejected ticket downgrades to
 * a full handshake inside the same connection attempt and resumption failure is never
 * a connection error. If the server issues no tickets the cache, cache simply stays empty.
 */

export const TLS_SESSION_CACHE_MAX_SIZE_BYTES = 100 * 1024 * 1024 // 100MB
export const TLS_SESSION_CACHE_MAX_AGE_MS = 60 * 60 * 1000
export function getTlsSessionCacheMaxEntries(): number {
  // 4x tenant config cache max entries due to longer TTL
  return 4 * TENANT_CONFIG_CACHE_MAX_ITEMS
}

type SessionCache = DisposableCache<string, Buffer, LruCacheSetOptions<string, Buffer>>

let sessionCache: SessionCache | undefined

// Lazy initialization to avoid metric observables if feature is unused.
function getSessionCache(): SessionCache {
  if (!sessionCache) {
    sessionCache = createLruCache<string, Buffer>(TLS_SESSION_CACHE_NAME, {
      max: getTlsSessionCacheMaxEntries(),
      maxSize: TLS_SESSION_CACHE_MAX_SIZE_BYTES,
      sizeCalculation: (session) => Math.max(session.length, 1),
      ttl: TLS_SESSION_CACHE_MAX_AGE_MS,
    })
  }

  return sessionCache
}

export function storeTlsSession(key: string, session: Buffer): void {
  getSessionCache().set(key, session)
}

export function getTlsSession(key: string): Buffer | undefined {
  return getSessionCache().get(key)
}

export function clearTlsSessionCache(): void {
  sessionCache?.dispose()
  sessionCache = undefined
}

export function getTlsSessionCacheSize(): number {
  return sessionCache?.getStats().entries ?? 0
}

export function installTlsSessionInjection(ssl: ConnectionOptions, key: string): void {
  if (Object.getOwnPropertyDescriptor(ssl, 'session')) {
    return
  }

  Object.defineProperty(ssl, 'session', {
    // enumerable to copy into tls.connect options
    enumerable: true,
    configurable: true,
    get: () => getTlsSession(key),
  })
}

export function attachTlsSessionCapture(stream: unknown, key: string): void {
  const socket = stream as
    | { on?: (event: string, listener: (arg: Buffer) => void) => unknown }
    | undefined
  if (!socket || typeof socket.on !== 'function') {
    return
  }

  socket.on('session', (session: Buffer) => {
    storeTlsSession(key, session)
  })
}

const disabledMetrics = { recordMetrics: false } as const

// Record whether the server actually resumed the offered session
export function observeTlsSessionResumption(stream: unknown, key: string): void {
  const socket = stream as
    | {
        once?: (event: string, listener: () => void) => unknown
        isSessionReused?: () => boolean
      }
    | undefined
  if (
    !socket ||
    typeof socket.once !== 'function' ||
    typeof socket.isSessionReused !== 'function'
  ) {
    return
  }

  const offered = getSessionCache().get(key, disabledMetrics) !== undefined

  socket.once('secureConnect', () => {
    const reused = socket.isSessionReused?.() === true
    recordTlsSessionResumption(reused ? 'resumed' : offered ? 'rejected' : 'uncached')
  })
}

// Dependency contract to fail in CI if pg changes between upgrades.
type PgClientInternals = {
  connectionParameters?: { host?: string; port?: number; ssl?: unknown }
  connection?: {
    once?: (event: string, listener: () => void) => unknown
    stream?: unknown
  }
}

type TlsSessionResumptionClientCtor = new (
  config?: ConstructorParameters<typeof Client>[0]
) => Client

let cachedClientClass: TlsSessionResumptionClientCtor | undefined

// Client that resumes TLS sessions per host:port.
export function getTlsSessionResumptionClient(): TlsSessionResumptionClientCtor {
  if (!cachedClientClass) {
    cachedClientClass = class TlsSessionResumptionClient extends Client {
      constructor(config?: ConstructorParameters<typeof Client>[0]) {
        super(config)

        const internals = this as unknown as PgClientInternals
        const ssl = internals.connectionParameters?.ssl
        if (!ssl || typeof ssl !== 'object') {
          return
        }

        const key = `${internals.connectionParameters?.host}:${internals.connectionParameters?.port}`
        installTlsSessionInjection(ssl as ConnectionOptions, key)

        const connection = internals.connection
        connection?.once?.('sslconnect', () => {
          attachTlsSessionCapture(connection.stream, key)
          observeTlsSessionResumption(connection.stream, key)
        })
      }
    }
  }

  return cachedClientClass
}
