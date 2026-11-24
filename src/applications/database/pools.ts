import {
  attachPoolErrorHandler,
  isConnectionStateError,
} from '@internal/database/postgres/pool-errors'
import { buildSearchPath } from '@internal/database/postgres/sql'
import { getSslSettings } from '@internal/database/postgres/ssl'
import { installPostgresTypeParsers } from '@internal/database/postgres/type-parsers'
import { getLogger } from '@platformatic/globals'
import { Pool, type PoolClient, type QueryResult } from 'pg'
import type { StorageConfigType } from '../../config.js'
import { DatabaseWattError } from './errors.js'
import type {
  BatchResponse,
  BatchStatement,
  DatabasePoolTarget,
  RawQueryResponse,
} from './protocol.js'

// pg type parsers are module-global and this application runs in its own worker
// thread, so they have to be installed here as well as in the storage workers.
// Statements run by runQuery skip them (rows go back unparsed); BEGIN, COMMIT and
// the other session statements still use them.
installPostgresTypeParsers()

type RawRow = Array<string | null>

const passThrough = (value: string) => value

// Every column comes back as the text PostgreSQL sent: the storage worker parses it.
const rawTypes = {
  getTypeParser: () => passThrough,
}

type PoolEntry = {
  config: DatabasePoolTarget
  lastUsedAt: number
  pool: Pool
}

export type PoolRegistryStats = {
  pools: number
  totalConnections: number
  inUseConnections: number
  waitingRequests: number
}

export class PoolRegistry {
  private readonly config: StorageConfigType
  private readonly pools = new Map<string, PoolEntry>()
  private readonly retiringPools = new Set<Promise<void>>()
  private readonly searchPath: string
  private pendingGlobalAcquisitions = 0
  private cachedStats?: PoolRegistryStats
  private readonly evictionInterval: NodeJS.Timeout

  constructor(config: StorageConfigType) {
    this.config = config
    this.searchPath = buildSearchPath(config.dbSearchPath).join(',')
    this.evictionInterval = setInterval(
      () => {
        void this.evictIdlePools()
      },
      Math.max(config.databaseWattPoolIdleTimeout, 1_000)
    )
    this.evictionInterval.unref()
  }

  async query(
    destination: DatabasePoolTarget,
    sql: string,
    values: unknown[] | undefined,
    onClient?: (client: PoolClient) => void
  ): Promise<RawQueryResponse> {
    const client = await this.acquire(destination)
    let releaseError: Error | undefined

    try {
      onClient?.(client)
      const result = await runQuery(client, sql, values)
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

  async acquire(destination: DatabasePoolTarget): Promise<PoolClient> {
    const entry = this.getOrCreatePool(destination)
    this.assertCanAcquire(entry)

    // An idle client is handed out without waiting, so there is nothing to time out.
    if (entry.pool.idleCount > entry.pool.waitingCount) {
      entry.lastUsedAt = Date.now()
      return entry.pool.connect()
    }

    this.pendingGlobalAcquisitions++
    entry.lastUsedAt = Date.now()

    const connecting = entry.pool.connect()
    let timedOut = false
    let timeout: NodeJS.Timeout | undefined

    try {
      return await new Promise<PoolClient>((resolve, reject) => {
        timeout = setTimeout(() => {
          timedOut = true
          reject(
            new DatabaseWattError('ACQUIRE_TIMEOUT', 'Timed out acquiring database connection')
          )
        }, this.config.databaseWattAcquireTimeout)
        timeout.unref()
        connecting.then(resolve, reject)
      })
    } finally {
      clearTimeout(timeout)
      this.pendingGlobalAcquisitions--
      entry.lastUsedAt = Date.now()

      if (timedOut) {
        // pg-pool keeps the pending request queued after we stop waiting for it.
        // Hand a late client straight back so it does not leak out of the pool.
        connecting.then(
          (client) => client.release(),
          () => undefined
        )
      }
    }
  }

  getStats(): PoolRegistryStats {
    if (this.cachedStats) {
      return this.cachedStats
    }

    let inUseConnections = 0
    let totalConnections = 0
    let waitingRequests = 0

    for (const entry of this.pools.values()) {
      inUseConnections += Math.max(entry.pool.totalCount - entry.pool.idleCount, 0)
      totalConnections += entry.pool.totalCount
      waitingRequests += entry.pool.waitingCount
    }

    const stats = {
      inUseConnections,
      pools: this.pools.size,
      totalConnections,
      waitingRequests,
    }
    this.cachedStats = stats
    queueMicrotask(() => {
      this.cachedStats = undefined
    })

    return stats
  }

  async close(): Promise<void> {
    clearInterval(this.evictionInterval)
    const pools = [...this.pools.values()].map((entry) => entry.pool)
    this.pools.clear()
    this.cachedStats = undefined
    await Promise.allSettled([...pools.map((pool) => pool.end()), ...this.retiringPools])
  }

  private getOrCreatePool(destination: DatabasePoolTarget): PoolEntry {
    const existing = this.pools.get(destination.id)
    if (existing && hasSameConnectionTarget(existing.config, destination)) {
      if (existing.config.maxConnections !== destination.maxConnections) {
        this.resizePool(existing, destination)
      }
      existing.lastUsedAt = Date.now()
      return existing
    }

    if (!existing && this.pools.size >= this.config.databaseWattMaxActivePools) {
      throw new DatabaseWattError('BUSY', 'Maximum active destination pools reached')
    }

    const pool = attachPoolErrorHandler(
      new Pool({
        application_name: this.config.databaseApplicationName,
        connectionString: destination.connectionString,
        // pg-pool applies this to queued checkouts as well as new connections, so it
        // must not undercut DATABASE_WATT_ACQUIRE_TIMEOUT, which bounds the wait.
        connectionTimeoutMillis: Math.max(
          this.config.databaseConnectionTimeout,
          this.config.databaseWattAcquireTimeout
        ),
        idleTimeoutMillis: this.config.databaseWattPoolIdleTimeout,
        max: this.getPoolMax(destination),
        min: 0,
        // Same session defaults as the direct tenant pools: external poolers get
        // search_path per transaction through the scope statement instead.
        options: destination.isExternalPool ? undefined : `-c search_path=${this.searchPath}`,
        pipeline: this.config.databaseEnablePipelining,
        ssl: getSslSettings({
          connectionString: destination.connectionString,
          databaseSSLRootCert: this.config.databaseSSLRootCert,
        }),
      }),
      (error) => {
        getLogger({ throwOnMissing: false })?.warn(
          {
            err: error,
            type: 'db',
            tenantId: destination.id,
            project: destination.id,
          },
          '[DatabaseWatt] Idle destination pg client error'
        )
      }
    )

    const entry = {
      config: destination,
      lastUsedAt: Date.now(),
      pool,
    }

    if (existing) {
      this.retirePool(existing.pool)
    }

    this.pools.set(destination.id, entry)
    this.cachedStats = undefined
    return entry
  }

  private getPoolMax(destination: DatabasePoolTarget): number {
    return Math.max(
      Math.min(destination.maxConnections, this.config.databaseWattDestinationMaxConnections),
      1
    )
  }

  // Capacity changes (tenant max_connections or cluster size) resize the live pool
  // instead of replacing it, so in-flight clients and warm connections survive.
  private resizePool(entry: PoolEntry, destination: DatabasePoolTarget): void {
    const previousMax = entry.pool.options.max
    const nextMax = this.getPoolMax(destination)
    entry.config = destination

    if (previousMax === nextMax) {
      return
    }

    entry.pool.options.max = nextMax
    if (nextMax > previousMax) {
      ;(entry.pool as Pool & { _pulseQueue?: () => void })._pulseQueue?.()
    }
  }

  private assertCanAcquire(entry: PoolEntry): void {
    if (this.pendingGlobalAcquisitions >= this.config.databaseWattGlobalAcquireQueueLimit) {
      throw new DatabaseWattError('BUSY', 'Global acquisition queue is full')
    }

    const { pool } = entry
    if (pool.waitingCount >= this.config.databaseWattDestinationAcquireQueueLimit) {
      throw new DatabaseWattError('BUSY', 'Destination acquisition queue is full')
    }

    // Only a request that makes pg-pool open a new connection spends global budget.
    // A request for a pool that is already at its own max waits in that pool's
    // queue (bounded above) for a connection to be released.
    const opensConnection = pool.idleCount === 0 && pool.totalCount < pool.options.max
    if (!opensConnection) {
      return
    }

    if (this.getStats().totalConnections < this.config.databaseWattGlobalMaxConnections) {
      return
    }

    if (this.reclaimIdlePools(entry) && this.hasGlobalConnectionBudget()) {
      return
    }

    throw new DatabaseWattError('BUSY', 'Global connection budget is exhausted')
  }

  private hasGlobalConnectionBudget(): boolean {
    this.cachedStats = undefined
    return this.getStats().totalConnections < this.config.databaseWattGlobalMaxConnections
  }

  // Ends least recently used pools whose connections are all idle so another
  // destination can open one. Returns whether any pool was reclaimed.
  private reclaimIdlePools(target: PoolEntry): boolean {
    const candidates = [...this.pools.entries()]
      .filter(([, entry]) => {
        return (
          entry !== target &&
          entry.pool.totalCount > 0 &&
          entry.pool.totalCount === entry.pool.idleCount &&
          entry.pool.waitingCount === 0
        )
      })
      .sort(([, left], [, right]) => left.lastUsedAt - right.lastUsedAt)

    let reclaimed = false
    for (const [destination, entry] of candidates) {
      this.pools.delete(destination)
      this.retirePool(entry.pool)
      reclaimed = true

      // pg-pool drops idle clients from totalCount synchronously on end().
      if (this.hasGlobalConnectionBudget()) {
        break
      }
    }

    return reclaimed
  }

  private async evictIdlePools(): Promise<void> {
    const now = Date.now()
    const toEvict: Pool[] = []

    for (const [destination, entry] of this.pools) {
      if (now - entry.lastUsedAt < this.config.databaseWattPoolIdleTimeout) {
        continue
      }

      if (entry.pool.totalCount !== entry.pool.idleCount || entry.pool.waitingCount > 0) {
        continue
      }

      this.pools.delete(destination)
      this.cachedStats = undefined
      toEvict.push(entry.pool)
    }

    await Promise.allSettled(toEvict.map((pool) => pool.end()))
  }

  private retirePool(pool: Pool): void {
    this.cachedStats = undefined
    const retirement = pool
      .end()
      .catch((error) => {
        getLogger({ throwOnMissing: false })?.warn(
          { err: error, type: 'db' },
          '[DatabaseWatt] Failed to retire replaced destination pool'
        )
      })
      .finally(() => {
        this.retiringPools.delete(retirement)
      })
    this.retiringPools.add(retirement)
  }
}

function hasSameConnectionTarget(left: DatabasePoolTarget, right: DatabasePoolTarget): boolean {
  return (
    left.connectionString === right.connectionString && left.isExternalPool === right.isExternalPool
  )
}

// Runs a statement on behalf of a storage worker. Rows come back as arrays of raw text
// values and the storage worker parses them, so this thread does not build row objects.
export async function runQuery(
  client: PoolClient,
  sql: string,
  values?: unknown[]
): Promise<QueryResult<RawRow>> {
  return client.query<RawRow>({ text: sql, values, rowMode: 'array', types: rawTypes })
}

export function toQueryResponse(result: QueryResult<RawRow>): RawQueryResponse {
  const fields = new Array(result.fields.length)
  for (let i = 0; i < result.fields.length; i++) {
    const field = result.fields[i]
    fields[i] = { name: field.name, dataTypeID: field.dataTypeID }
  }

  return {
    fields,
    rows: result.rows,
    rowCount: result.rowCount || 0,
  }
}

/**
 * The response to a batch: the last statement's result, plus the results of the
 * statements flagged with `result`, in order.
 */
export function toBatchResponse(
  statements: BatchStatement[],
  results: QueryResult<RawRow>[]
): BatchResponse {
  const response: BatchResponse = toQueryResponse(results[results.length - 1])
  for (let i = 0; i < statements.length; i++) {
    if (statements[i].result) {
      response.results ??= []
      response.results.push(toQueryResponse(results[i]))
    }
  }
  return response
}

export class BeginError extends Error {
  constructor(readonly original: unknown) {
    super(original instanceof Error ? original.message : String(original), { cause: original })
    this.name = 'BeginError'
  }
}

/**
 * Runs BEGIN, `statements` and, when asked, COMMIT on one connection, returning the
 * result of every statement, in order. A failed BEGIN is thrown as a BeginError so callers can
 * retry it on another connection: nothing ran yet.
 *
 * On a pipelining client everything is written at once (one socket write, one round
 * trip). Otherwise the statements run one after the other, still with a single message
 * from the storage worker.
 */
export async function runBatch(
  client: PoolClient,
  beginStatement: string,
  statements: BatchStatement[],
  commit: boolean
): Promise<QueryResult<RawRow>[]> {
  if ((client as PoolClient & { pipeline?: boolean }).pipeline) {
    return runPipelinedBatch(client, beginStatement, statements, commit)
  }

  try {
    await client.query(beginStatement)
  } catch (error) {
    throw new BeginError(error)
  }

  const results = new Array<QueryResult<RawRow>>(statements.length)
  for (let i = 0; i < statements.length; i++) {
    results[i] = await runQuery(client, statements[i].sql, statements[i].values)
  }

  if (commit) {
    await client.query('COMMIT')
  }

  return results
}

async function runPipelinedBatch(
  client: PoolClient,
  beginStatement: string,
  statements: BatchStatement[],
  commit: boolean
): Promise<QueryResult<RawRow>[]> {
  const stream = (
    client as PoolClient & {
      connection?: { stream?: Partial<NodeJS.WritableStream> & { cork?(): void; uncork?(): void } }
    }
  ).connection?.stream
  const pending: Promise<unknown>[] = []

  // pg submits pipelined queries synchronously: corking merges them into one write.
  stream?.cork?.()
  try {
    pending.push(client.query(beginStatement))
    for (const statement of statements) {
      pending.push(runQuery(client, statement.sql, statement.values))
    }
    if (commit) {
      pending.push(client.query('COMMIT'))
    }
  } finally {
    stream?.uncork?.()
  }

  const settled = await Promise.allSettled(pending)

  if (settled[0].status === 'rejected') {
    throw new BeginError(settled[0].reason)
  }

  // The first failure is the cause; later statements only report the aborted transaction.
  for (const outcome of settled) {
    if (outcome.status === 'rejected') {
      throw outcome.reason
    }
  }

  const results = new Array<QueryResult<RawRow>>(statements.length)
  for (let i = 0; i < statements.length; i++) {
    // settled[0] is BEGIN.
    results[i] = (settled[i + 1] as PromiseFulfilledResult<QueryResult<RawRow>>).value
  }
  return results
}
