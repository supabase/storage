import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg'
import type { DatabaseConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import { getSslSettings } from './ssl.js'
import type { DestinationConfig, QueryResponse } from './types.js'

type PoolEntry = {
  config: DestinationConfig
  lastUsedAt: number
  pool: Pool
}

export class PoolRegistry {
  private readonly config: DatabaseConfig
  private readonly pools = new Map<string, PoolEntry>()
  private pendingGlobalAcquisitions = 0
  private readonly evictionInterval: NodeJS.Timeout

  constructor(config: DatabaseConfig) {
    this.config = config
    this.evictionInterval = setInterval(() => {
      void this.evictIdlePools()
    }, Math.max(config.idlePoolTimeoutMs, 1_000))
    this.evictionInterval.unref()
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    destination: DestinationConfig,
    sql: string,
    values: unknown[] | undefined,
    onClient?: (client: PoolClient) => void
  ): Promise<QueryResponse<T>> {
    const client = await this.acquire(destination)
    let releaseError: Error | undefined

    try {
      onClient?.(client)
      const result = await runQuery<T>(client, sql, values)
      return toQueryResponse(result)
    } catch (error) {
      if (isConnectionStateError(error)) {
        releaseError = error instanceof Error ? error : new Error(String(error))
      }
      throw error
    } finally {
      client.release(releaseError)
    }
  }

  async acquire(destination: DestinationConfig): Promise<PoolClient> {
    const entry = this.getOrCreatePool(destination)
    this.assertCanAcquire(entry)
    this.pendingGlobalAcquisitions++

    const timeout = createTimeout(this.config.acquireTimeoutMs)
    try {
      return await Promise.race([
        entry.pool.connect(),
        timeout.promise.then(() => {
          throw new DatabaseWattError('ACQUIRE_TIMEOUT', 'Timed out acquiring database connection')
        }),
      ])
    } finally {
      timeout.clear()
      this.pendingGlobalAcquisitions--
      entry.lastUsedAt = Date.now()
    }
  }

  getStats(): { pools: number; totalConnections: number; waitingRequests: number } {
    let totalConnections = 0
    let waitingRequests = 0

    for (const entry of this.pools.values()) {
      totalConnections += entry.pool.totalCount
      waitingRequests += entry.pool.waitingCount
    }

    return {
      pools: this.pools.size,
      totalConnections,
      waitingRequests,
    }
  }

  async close(): Promise<void> {
    clearInterval(this.evictionInterval)
    const pools = [...this.pools.values()].map((entry) => entry.pool)
    this.pools.clear()
    await Promise.allSettled(pools.map((pool) => pool.end()))
  }

  private getOrCreatePool(destination: DestinationConfig): PoolEntry {
    const existing = this.pools.get(destination.id)
    if (existing) {
      existing.lastUsedAt = Date.now()
      return existing
    }

    if (this.pools.size >= this.config.maxActivePools) {
      throw new DatabaseWattError('BUSY', 'Maximum active destination pools reached')
    }

    const maxConnections = Math.max(
      Math.min(destination.maxConnections, this.config.destinationMaxConnections),
      1
    )
    const pool = new Pool({
      application_name: this.config.applicationName,
      connectionString: destination.connectionString,
      connectionTimeoutMillis: this.config.connectionTimeoutMs,
      idleTimeoutMillis: this.config.idlePoolTimeoutMs,
      max: maxConnections,
      min: 0,
      ssl: getSslSettings(destination.connectionString, this.config.rootCert),
    })

    const entry = {
      config: destination,
      lastUsedAt: Date.now(),
      pool,
    }

    this.pools.set(destination.id, entry)
    return entry
  }

  private assertCanAcquire(entry: PoolEntry): void {
    const stats = this.getStats()

    if (this.pendingGlobalAcquisitions >= this.config.globalAcquireQueueLimit) {
      throw new DatabaseWattError('BUSY', 'Global acquisition queue is full')
    }

    if (entry.pool.waitingCount >= this.config.destinationAcquireQueueLimit) {
      throw new DatabaseWattError('BUSY', 'Destination acquisition queue is full')
    }

    const targetHasIdleConnection = entry.pool.idleCount > 0
    if (!targetHasIdleConnection && stats.totalConnections >= this.config.globalMaxConnections) {
      throw new DatabaseWattError('BUSY', 'Global connection budget is exhausted')
    }
  }

  private async evictIdlePools(): Promise<void> {
    const now = Date.now()
    const toEvict: Pool[] = []

    for (const [destination, entry] of this.pools) {
      if (now - entry.lastUsedAt < this.config.idlePoolTimeoutMs) {
        continue
      }

      if (entry.pool.totalCount !== entry.pool.idleCount || entry.pool.waitingCount > 0) {
        continue
      }

      this.pools.delete(destination)
      toEvict.push(entry.pool)
    }

    await Promise.allSettled(toEvict.map((pool) => pool.end()))
  }
}

export async function runQuery<T extends QueryResultRow = QueryResultRow>(
  client: PoolClient,
  sql: string,
  values?: unknown[]
): Promise<QueryResult<T>> {
  return client.query<T>(sql, values)
}

export function toQueryResponse<T extends QueryResultRow = QueryResultRow>(
  result: QueryResult<T>
): QueryResponse<T> {
  return {
    rows: result.rows,
    rowCount: result.rowCount || 0,
  }
}

export function isConnectionStateError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  const code = (error as { code?: string }).code
  if (typeof code === 'string' && code.startsWith('08')) {
    return true
  }

  return (
    error.message.startsWith('received invalid response:') ||
    error.message.startsWith('Received unexpected ') ||
    error.message.startsWith('Unknown authenticationOk message type')
  )
}

function createTimeout(timeoutMs: number): { clear: () => void; promise: Promise<void> } {
  let timeout: NodeJS.Timeout | undefined
  const promise = new Promise<void>((resolve) => {
    timeout = setTimeout(resolve, timeoutMs)
    timeout.unref()
  })

  return {
    clear: () => {
      if (timeout) {
        clearTimeout(timeout)
      }
    },
    promise,
  }
}
