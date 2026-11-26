import type { Lease, LeaseDisposeReason } from '@internal/cache'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { Pool, type PoolClient, type QueryResult, type QueryResultRow } from 'pg'
import { getConfig } from '../../config'
import type {
  DatabaseExecutor,
  DatabaseQueryArgument,
  DatabaseStatement,
  DatabaseTransaction,
  DatabaseTransactionalExecutor,
  ScopedStatement,
  TenantConnection,
  TransactionOptions,
} from './connection'
import {
  PoolManager,
  PoolRebalanceOptions,
  PoolStats,
  type PoolStrategy,
  PoolStrategySettings,
  searchPath,
  TenantConnectionOptions,
} from './pool'
import { assertValidSignal } from './postgres/asserts'
import { BatchQuery } from './postgres/batch-query'
import { cancelQuery } from './postgres/cancellation'
import {
  attachPoolErrorHandler,
  checkoutClient,
  createAbortError,
  isConnectionStateError,
  isConnectionTimeoutError,
  isRetryableTransactionSetupError,
  markClientDisposable,
  type PgClientCheckout,
  shouldDisposeClient,
} from './postgres/pool-errors'
import { buildScopeStatement } from './postgres/scope'
import {
  buildBeginStatement,
  normalizeStatement,
  normalizeStatementTimeoutMs,
} from './postgres/sql'
import { getSslSettings } from './postgres/ssl'
import { installPostgresTypeParsers } from './postgres/type-parsers'
import {
  createTlsSessionSlot,
  installTlsSessionResumption,
  TlsSessionResumptionClient,
  type TlsSessionSlot,
} from './tls-session-resumption'

const {
  databaseApplicationName,
  databaseConnectionTimeout,
  databaseEnablePipelining,
  databaseFreePoolAfterInactivity,
  databaseMaxConnections,
  databasePoolDrainTimeout,
  databaseSSLRootCert,
  databaseStatementTimeout,
  databaseTlsSessionResumption,
} = getConfig()

installPostgresTypeParsers()

interface PgTransactionOptions {
  searchPath?: string
  statementTimeoutMs?: number
  cacheScope?: object
}

type PgBeginTransactionOptions = TransactionOptions & PgTransactionOptions

const defaultStatementTimeoutMs = normalizeStatementTimeoutMs(databaseStatementTimeout)

// Shared frozen options for the common transaction() call without caller options.
const defaultBeginTransactionOptions: PgBeginTransactionOptions | undefined =
  defaultStatementTimeoutMs !== undefined
    ? Object.freeze({ statementTimeoutMs: defaultStatementTimeoutMs })
    : undefined
const externalPoolSearchPath = searchPath.join(',')
const defaultExternalPoolBeginTransactionOptions: PgBeginTransactionOptions = Object.freeze({
  ...defaultBeginTransactionOptions,
  searchPath: externalPoolSearchPath,
})

interface PgPoolErrorContext {
  message: string
  tenantId?: string
  project?: string
}

const poolDrainCheckIntervalMs = 200
const transactionSetupMaxAttempts = 11
const transactionSetupTotalBudgetMs = 3000
const transactionSetupRetryMinDelayMs = 50
const transactionSetupRetryMaxDelayMs = 200

export class PgPoolStrategy implements PoolStrategy {
  protected pool?: Pool
  protected tlsSession?: TlsSessionSlot
  private disposed = false
  private executor?: PgPoolExecutor
  private executorPool?: Pool

  constructor(protected readonly options: PoolStrategySettings) {}

  acquire(): PgPoolExecutor {
    const pool = this.getPool()
    if (!this.executor || this.executorPool !== pool) {
      this.executor = new PgPoolExecutor(pool)
      this.executorPool = pool
    }
    return this.executor
  }

  async dispose(reason: LeaseDisposeReason): Promise<void> {
    this.disposed = true
    const originalPool = this.pool
    this.executor = undefined
    this.executorPool = undefined

    if (!originalPool) {
      return
    }

    // Keep the physical pool visible to stats while it drains. Disposal blocks reuse.
    try {
      await this.drainPool(originalPool, reason)
    } finally {
      if (this.pool === originalPool) {
        this.pool = undefined
      }
    }
  }

  rebalance(options: PoolRebalanceOptions): void {
    if (this.disposed) {
      return
    }

    let capacityChanged = false

    if (options.clusterSize !== undefined && options.clusterSize !== 0) {
      capacityChanged = this.options.clusterSize !== options.clusterSize
      this.options.clusterSize = options.clusterSize
    }

    if (options.maxConnections !== undefined) {
      capacityChanged = capacityChanged || this.options.maxConnections !== options.maxConnections
      this.options.maxConnections = options.maxConnections
    }

    if (capacityChanged) {
      this.updatePoolMax()
    }
  }

  private updatePoolMax(): void {
    if (!this.pool) {
      return
    }

    const previousMax = this.pool.options.max
    const nextMax = this.getSettings().maxConnections
    if (nextMax === previousMax) {
      return
    }

    this.pool.options.max = nextMax
    if (nextMax > previousMax) {
      pulsePgPoolQueue(this.pool)
    }
  }

  getPoolStats(): PoolStats | null {
    if (!this.pool) {
      return null
    }

    return {
      used: Math.max(this.pool.totalCount - this.pool.idleCount, 0),
      total: this.pool.totalCount,
    }
  }

  protected getPool(): Pool {
    if (this.disposed) {
      throw ERRORS.InternalError(undefined, 'Cannot acquire from a disposed pool strategy')
    }

    if (!this.pool) {
      this.pool = this.createPool()
    }

    return this.pool
  }

  protected getSettings() {
    const numWorkers = Math.max(this.options.numWorkers ?? 1, 1)
    const clusterSize = this.options.clusterSize || 0
    let maxConnection = this.options.maxConnections || databaseMaxConnections

    const divisor = Math.max(clusterSize, 1) * numWorkers
    if (divisor > 1) {
      maxConnection = Math.ceil(maxConnection / divisor) || 1
    }

    return {
      ...this.options,
      idleTimeoutMillis: databaseFreePoolAfterInactivity,
      maxConnections: maxConnection,
      searchPath: this.options.isExternalPool ? undefined : searchPath,
    }
  }

  protected createPool(): Pool {
    const settings = this.getSettings()
    const sslSettings = getSslSettings({
      connectionString: settings.dbUrl,
      databaseSSLRootCert,
    })

    const ssl = sslSettings ? { ...sslSettings } : undefined
    if (databaseTlsSessionResumption && ssl) {
      this.tlsSession ??= createTlsSessionSlot()
      installTlsSessionResumption(ssl, this.tlsSession)
    }

    return attachPoolErrorHandler(
      new Pool({
        min: 0,
        max: settings.maxConnections,
        connectionString: settings.dbUrl,
        connectionTimeoutMillis: databaseConnectionTimeout,
        idleTimeoutMillis: settings.idleTimeoutMillis,
        pipeline: databaseEnablePipelining,
        ssl,
        Client: databaseTlsSessionResumption && ssl ? TlsSessionResumptionClient : undefined,
        application_name: databaseApplicationName,
        options: settings.searchPath
          ? `-c search_path=${settings.searchPath.join(',')}`
          : undefined,
      }),
      createPgPoolErrorHandler({
        message: '[PgPoolStrategy] Idle pg client error',
        tenantId: settings.tenantId,
        project: settings.tenantId,
      })
    )
  }

  private async drainPool(pool: Pool, reason: LeaseDisposeReason): Promise<void> {
    const startedAt = Date.now()
    const deadline = startedAt + databasePoolDrainTimeout

    // Match the legacy Knex pool drain: let queued acquires settle before ending.
    while ((pool.waitingCount ?? 0) > 0 && !isPoolEnding(pool)) {
      const remainingMs = deadline - Date.now()

      if (remainingMs <= 0) {
        this.logPoolDrainTimeout(pool, reason, Date.now() - startedAt)
        break
      }

      await wait(Math.min(poolDrainCheckIntervalMs, remainingMs))
    }

    if (isPoolEnding(pool)) {
      return
    }

    await pool.end()
  }

  private logPoolDrainTimeout(pool: Pool, reason: LeaseDisposeReason, elapsedMs: number): void {
    const metadata = {
      reason,
      drainTimeoutMs: databasePoolDrainTimeout,
      elapsedMs,
      ...getPoolWorkStats(pool),
    }

    logSchema.warning(logger, '[PgPoolStrategy] Timed out waiting for pg pool to drain', {
      type: 'db',
      tenantId: this.options.tenantId,
      project: this.options.tenantId,
      metadata: JSON.stringify(metadata),
    })
  }
}

function createPgPoolErrorHandler(context: PgPoolErrorContext): (error: Error) => void {
  return (error) => {
    logSchema.warning(logger, context.message, {
      type: 'db',
      tenantId: context.tenantId,
      project: context.project,
      error,
    })
  }
}

function getPoolWorkStats(pool: Pool): {
  waitingCount: number
  activeCount: number
  totalCount: number
  idleCount: number
} {
  const waitingCount = pool.waitingCount ?? 0
  const totalCount = pool.totalCount ?? 0
  const idleCount = pool.idleCount ?? 0

  return {
    waitingCount,
    activeCount: Math.max(totalCount - idleCount, 0),
    totalCount,
    idleCount,
  }
}

function isPoolEnding(pool: Pool): boolean {
  return Boolean(pool.ending || pool.ended)
}

function wait(timeoutMs: number): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, timeoutMs)
    timeout.unref?.()
  })
}

function pulsePgPoolQueue(pool: Pool): void {
  ;(pool as Pool & { _pulseQueue?: () => void })._pulseQueue?.()
}

export class PgPoolManager extends PoolManager<PgPoolStrategy> {
  protected newPool(settings: PoolStrategySettings): PgPoolStrategy {
    return new PgPoolStrategy(settings)
  }
}

function releasePgClient(client: PoolClient, error?: Error): void {
  client.release(error)

  if (error && client.pipeline) {
    // pg waits for pipelined queries in end(). Discarded clients must close
    // immediately so a failed cancel cannot block queued pool checkouts.
    client.connection.stream.destroy()
  }
}

export class PgPoolExecutor implements DatabaseTransactionalExecutor {
  constructor(private readonly pool: Pool) {}

  getCacheScope(): object {
    return this.pool
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    assertValidSignal(getQuerySignal(options))

    let checkout: PgClientCheckout | undefined
    let releaseError: Error | undefined

    try {
      checkout = await checkoutClient(this.pool)
      checkout.throwIfErrored()
      const result = await runPgQuery<T>(checkout.client, statement, options)
      checkout.throwIfErrored()
      return result
    } catch (e) {
      if (!checkout) {
        if (isConnectionTimeoutError(e)) {
          throw ERRORS.DatabaseTimeout(e)
        }

        throw e
      }

      releaseError = checkout.releaseErrorForQuery(e)
      throw releaseError ?? e
    } finally {
      checkout?.release(releaseError)
    }
  }

  async beginTransaction(options?: PgBeginTransactionOptions): Promise<PgTransaction> {
    let checkout: PgClientCheckout
    try {
      checkout = await checkoutClient(this.pool)
    } catch (e) {
      if (isConnectionTimeoutError(e)) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }

    const transaction = new PgTransaction(checkout.client, checkout, {
      searchPath: options?.searchPath,
      statementTimeoutMs: options?.statementTimeoutMs ?? options?.timeout,
      cacheScope: this.getCacheScope(),
    })

    try {
      await transaction.runSetupQuery(buildBeginStatement(options))
      return transaction
    } catch (e) {
      if (!transaction.isCompleted()) {
        checkout.release(checkout.releaseErrorForFinalizer(e))
      }
      throw e
    }
  }

  /**
   * Runs `beginStatement`, `statements` and COMMIT as one transaction on one client and
   * returns every statement's result, in order, in one round trip (see runPgTransaction). A
   * failed checkout or BEGIN is marked as a setup failure: nothing ran, so the caller may
   * retry it on another client.
   */
  async runTransaction(
    beginStatement: string,
    statements: DatabaseStatement[],
    signal?: AbortSignal
  ): Promise<QueryResult[]> {
    assertValidSignal(signal)

    let checkout: PgClientCheckout
    try {
      checkout = await checkoutClient(this.pool)
    } catch (e) {
      if (isConnectionTimeoutError(e)) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw markTransactionSetupFailure(e)
    }

    let releaseError: Error | undefined
    try {
      try {
        checkout.throwIfErrored()
      } catch (e) {
        throw markTransactionSetupFailure(e)
      }

      const results = await runPgTransaction(checkout.client, beginStatement, statements, signal)
      checkout.throwIfErrored()
      return results
    } catch (e) {
      // As in beginTransaction() and commit(): a failed BEGIN or COMMIT leaves the client's
      // state uncertain, so it is discarded.
      if (
        transactionSetupFailures.has(e as object) ||
        transactionFinalizerFailures.has(e as object)
      ) {
        releaseError = checkout.releaseErrorForFinalizer(e)
        throw e
      }

      releaseError = checkout.releaseErrorForQuery(e)
      throw releaseError ?? e
    } finally {
      checkout.release(releaseError)
    }
  }
}

export class PgTransaction implements DatabaseTransaction {
  private completed = false
  private pendingSearchPath?: string
  private pendingStatementTimeoutMs?: number
  private readonly cacheScope: object

  constructor(
    private readonly client: PoolClient,
    private readonly checkout?: PgClientCheckout,
    options: PgTransactionOptions = {}
  ) {
    this.pendingSearchPath = options.searchPath || undefined
    this.pendingStatementTimeoutMs = normalizeStatementTimeoutMs(options.statementTimeoutMs)
    this.cacheScope = options.cacheScope ?? client
  }

  getCacheScope(): object {
    return this.cacheScope
  }

  isCompleted(): boolean {
    return this.completed
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    return this.runQuery(statement, options, true)
  }

  async runSetupQuery<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    // Setup statements must not consume deferred settings before scope can fold them in.
    return this.runQuery(statement, options, false)
  }

  // Hands deferred settings to the caller (setScope) so they can be folded
  // into its next statement instead of costing separate set_config round trips.
  takePendingStatementTimeoutMs(): number | undefined {
    const timeoutMs = this.pendingStatementTimeoutMs
    this.pendingStatementTimeoutMs = undefined
    return timeoutMs
  }

  takePendingSearchPath(): string | undefined {
    const pendingSearchPath = this.pendingSearchPath
    this.pendingSearchPath = undefined
    return pendingSearchPath
  }

  private async runQuery<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options: DatabaseQueryArgument | undefined,
    applyPendingSettings: boolean
  ): Promise<QueryResult<T>> {
    if (this.completed) {
      throw new Error('Cannot query a completed transaction')
    }

    const signal = getQuerySignal(options)

    try {
      assertValidSignal(signal)
      this.checkout?.throwIfErrored()

      if (
        applyPendingSettings &&
        (this.pendingSearchPath !== undefined || this.pendingStatementTimeoutMs !== undefined)
      ) {
        await this.applyPendingSettingsBeforeQuery(signal)
      }

      const result = await runPgQuery<T>(this.client, statement, options)
      this.checkout?.throwIfErrored()
      return result
    } catch (e) {
      const releaseError =
        this.checkout?.releaseErrorForQuery(e) ??
        (shouldDisposeClient(e) ? ensureError(e) : undefined)
      if (releaseError) {
        this.release(releaseError)
        throw releaseError
      }
      throw e
    }
  }

  async commit(): Promise<void> {
    if (this.completed) {
      return
    }

    let releaseError: Error | undefined
    try {
      await runPgQuery(this.client, 'COMMIT')
      this.checkout?.throwIfErrored()
    } catch (e) {
      // A failed transaction finalizer leaves cleanup state uncertain, so release
      // with the error even for recoverable SQLSTATEs such as 40001 at COMMIT.
      releaseError = this.checkout?.releaseErrorForFinalizer(e) ?? ensureError(e)
      throw e
    } finally {
      this.release(releaseError)
    }
  }

  async rollback(): Promise<void> {
    if (this.completed) {
      return
    }

    let releaseError: Error | undefined
    try {
      await runPgQuery(this.client, 'ROLLBACK')
      this.checkout?.throwIfErrored()
    } catch (e) {
      // A failed transaction finalizer leaves cleanup state uncertain, so release
      // with the error even for recoverable SQLSTATEs.
      releaseError = this.checkout?.releaseErrorForFinalizer(e) ?? ensureError(e)
      throw e
    } finally {
      this.release(releaseError)
    }
  }

  private release(releaseError?: Error): void {
    if (this.completed) {
      return
    }
    this.completed = true
    if (this.checkout) {
      this.checkout.release(releaseError)
      return
    }
    releasePgClient(this.client, releaseError)
  }

  private async applyPendingSettingsBeforeQuery(signal?: AbortSignal): Promise<void> {
    const timeoutMs = this.takePendingStatementTimeoutMs()
    const pendingSearchPath = this.takePendingSearchPath()

    const values: string[] = []
    if (timeoutMs) {
      values.push(`${timeoutMs}ms`)
    }
    if (pendingSearchPath) {
      values.push(pendingSearchPath)
    }

    await runPgQuery(
      this.client,
      {
        text: getPendingSettingsSql(timeoutMs, pendingSearchPath),
        values,
      },
      signal ? { signal } : undefined
    )
  }
}

const noopRelease = () => {}
const serializedJwtPayloads = new WeakMap<object, string>()

function serializeJwtPayload(payload: object): string {
  let serialized = serializedJwtPayloads.get(payload)
  if (serialized === undefined) {
    serialized = JSON.stringify(payload)
    serializedJwtPayloads.set(payload, serialized)
  }
  return serialized
}

export class PgTenantConnection implements TenantConnection {
  static poolManager = new PgPoolManager()
  public readonly role: string
  public readonly pool: PgPoolStrategy
  // Set only when the pools pipeline (DATABASE_ENABLE_PIPELINING): there BEGIN, the scope, the
  // statements and COMMIT go out in one write, one round trip. Otherwise StoragePgDB opens a
  // regular transaction, which costs a round trip per statement.
  public readonly singleStatementExecutor?: (options?: TransactionOptions) => DatabaseExecutor
  public readonly scopedBatch?: (statements: ScopedStatement[]) => Promise<QueryResult[]>
  private abortSignal?: AbortSignal
  private disposed = false
  private readonly headersPayload: string
  private userPayload?: string

  constructor(
    private readonly poolLease: Lease<PgPoolStrategy>,
    protected readonly options: TenantConnectionOptions,
    headersPayload?: string
  ) {
    this.pool = poolLease.value
    this.role = options.user.payload.role || 'anon'
    this.headersPayload = headersPayload ?? JSON.stringify(options.headers || {})

    if (databaseEnablePipelining) {
      this.singleStatementExecutor = this.pipelinedSingleStatementExecutor
      this.scopedBatch = this.pipelinedScopedBatch
    }
  }

  static async stop(): Promise<void> {
    const results = await PgTenantConnection.poolManager.destroyAll()
    const errors = results.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : []
    )
    if (errors.length > 0) {
      throw new AggregateError(errors, 'Failed to retire one or more tenant database pools')
    }
  }

  static create(options: TenantConnectionOptions): PgTenantConnection {
    return new this(PgTenantConnection.poolManager.getPool(options), options)
  }

  dispose(): void {
    if (this.disposed) {
      return
    }

    this.disposed = true
    this.poolLease.release()
  }

  setAbortSignal(signal: AbortSignal) {
    this.abortSignal = signal
  }

  getAbortSignal(): AbortSignal | undefined {
    return this.abortSignal
  }

  acquireExecutor(): PgPoolExecutor {
    this.assertNotDisposed()
    return this.pool.acquire()
  }

  getCacheScope(): object {
    return this.acquireExecutor().getCacheScope()
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    this.assertNotDisposed()
    return this.pool.acquire().query<T>(statement, options)
  }

  async beginTransaction(options?: TransactionOptions): Promise<PgTransaction> {
    return this.beginTransactionWithRetry(options)
  }

  asSuperUser() {
    this.assertNotDisposed()

    const tenantConnection = new PgTenantConnection(
      { value: this.pool, release: noopRelease },
      {
        ...this.options,
        user: this.options.superUser,
      },
      this.headersPayload
    )

    if (this.abortSignal) {
      tenantConnection.setAbortSignal(this.abortSignal)
    }

    return tenantConnection
  }

  async transaction(opts?: TransactionOptions): Promise<PgTransaction> {
    this.assertNotDisposed()

    try {
      const beginOptions = withDefaultTransactionSettings(opts, this.options.isExternalPool)
      return await this.beginTransactionWithRetry(beginOptions)
    } catch (e) {
      if (isConnectionTimeoutError(e)) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }
  }

  private beginTransactionWithRetry(opts?: PgBeginTransactionOptions): Promise<PgTransaction> {
    return withTransactionSetupRetry(
      () => this.beginTransactionForRequest(opts),
      isRetryableTransactionSetupError
    )
  }

  private beginTransactionForRequest(opts?: PgBeginTransactionOptions): Promise<PgTransaction> {
    this.assertNotDisposed()
    if (this.abortSignal?.aborted) {
      throw createAbortError()
    }
    // PgPoolExecutor derives the deferred statement_timeout from options.timeout.
    return this.pool.acquire().beginTransaction(opts)
  }

  private assertNotDisposed(): void {
    if (this.disposed) {
      throw createDisposedTenantConnectionError()
    }
  }

  async setScope(tnx: DatabaseExecutor) {
    let statementTimeoutMs: number | undefined
    let pendingSearchPath: string | undefined
    if (tnx instanceof PgTransaction) {
      statementTimeoutMs = tnx.takePendingStatementTimeoutMs()
      pendingSearchPath = tnx.takePendingSearchPath()
    }

    await tnx.query(this.buildScope(statementTimeoutMs, pendingSearchPath))
  }

  /**
   * An executor that runs one statement as a whole scoped transaction: the BEGIN and scope
   * that transaction() and setScope() would run, the statement, then COMMIT: one round trip.
   */
  private pipelinedSingleStatementExecutor(options?: TransactionOptions): DatabaseExecutor {
    this.assertNotDisposed()
    const settings = withDefaultTransactionSettings(options, this.options.isExternalPool)
    const scope = this.buildScope(
      normalizeStatementTimeoutMs(settings?.statementTimeoutMs ?? settings?.timeout),
      settings?.searchPath
    )

    return new PgSingleStatementExecutor(this, buildBeginStatement(settings), scope)
  }

  /**
   * Runs `statements` as one transaction in one round trip: BEGIN, the settings
   * transaction() applies, each statement after the scope it asks for (this
   * connection's or, when flagged `asSuperUser`, the super user's), and COMMIT. Scope
   * statements set the same keys, so each one fully replaces the one before it.
   */
  private async pipelinedScopedBatch(statements: ScopedStatement[]): Promise<QueryResult[]> {
    this.assertNotDisposed()
    if (statements.length === 0) {
      return []
    }

    const settings = withDefaultTransactionSettings(undefined, this.options.isExternalPool)
    const batch: DatabaseStatement[] = []
    const resultIndexes: number[] = []
    let superUser: PgTenantConnection | undefined
    let applied: PgTenantConnection | undefined

    for (const statement of statements) {
      if (statement.asSuperUser && !superUser) {
        superUser = this.asSuperUser()
      }
      const scoped = (statement.asSuperUser ? superUser : this) as PgTenantConnection
      if (scoped !== applied) {
        // Transaction-wide settings only need to be set once.
        batch.push(
          applied
            ? scoped.buildScope(undefined, undefined)
            : scoped.buildScope(
                normalizeStatementTimeoutMs(settings?.statementTimeoutMs),
                settings?.searchPath
              )
        )
        applied = scoped
      }
      resultIndexes.push(batch.length)
      batch.push({ text: statement.text, values: statement.values })
    }

    const results = await this.runPipelinedTransaction('BEGIN', batch, this.abortSignal)
    return resultIndexes.map((index) => results[index])
  }

  runPipelinedTransaction(
    beginStatement: string,
    statements: DatabaseStatement[],
    signal: AbortSignal | undefined
  ): Promise<QueryResult[]> {
    return withTransactionSetupRetry(() => {
      this.assertNotDisposed()
      if (this.abortSignal?.aborted || signal?.aborted) {
        throw createAbortError()
      }
      return this.pool.acquire().runTransaction(beginStatement, statements, signal)
    }, isRetryablePipelinedSetupError)
  }

  private buildScope(statementTimeoutMs: number | undefined, searchPath: string | undefined) {
    return buildScopeStatement(
      this.options,
      this.role,
      this.getUserPayload(),
      this.headersPayload,
      statementTimeoutMs,
      searchPath
    )
  }

  private getUserPayload(): string {
    this.userPayload ??= serializeJwtPayload(this.options.user.payload)
    return this.userPayload
  }
}

/**
 * Runs one statement, with the BEGIN and scope it was created with, as a whole transaction.
 * Used by StoragePgDB for operations that issue a single query.
 */
class PgSingleStatementExecutor implements DatabaseExecutor {
  private used = false

  constructor(
    private readonly connection: PgTenantConnection,
    private readonly beginStatement: string,
    private readonly scope: DatabaseStatement
  ) {}

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.used) {
      throw new Error('A single statement executor runs only one statement')
    }
    this.used = true

    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const results = await this.connection.runPipelinedTransaction(
      this.beginStatement,
      [this.scope, query],
      getQuerySignal(options)
    )
    return results[1] as QueryResult<T>
  }
}

async function withTransactionSetupRetry<T>(
  attempt: () => Promise<T>,
  isRetryable: (error: unknown) => boolean
): Promise<T> {
  const startedAt = performance.now()
  let delayMs = transactionSetupRetryMinDelayMs

  for (let attempts = 1; ; attempts++) {
    try {
      return await attempt()
    } catch (e) {
      // Full jitter with exponential backoff: 50-100, 100-200, then 200ms.
      const sleepMs = Math.min(delayMs * (1 + Math.random()), transactionSetupRetryMaxDelayMs)
      const elapsedMs = performance.now() - startedAt

      if (
        !isRetryable(e) ||
        attempts >= transactionSetupMaxAttempts ||
        elapsedMs + sleepMs >= transactionSetupTotalBudgetMs
      ) {
        throw e
      }

      await wait(sleepMs)
      delayMs = Math.min(delayMs * 2, transactionSetupRetryMaxDelayMs)
    }
  }
}

// Errors from a checkout or a BEGIN that failed before anything else ran, and from a
// COMMIT that failed after every statement succeeded.
const transactionSetupFailures = new WeakSet<object>()
const transactionFinalizerFailures = new WeakSet<object>()

function markTransactionSetupFailure(error: unknown): unknown {
  if (typeof error === 'object' && error !== null) {
    transactionSetupFailures.add(error)
  }
  return error
}

function markTransactionFinalizerFailure(error: unknown): unknown {
  if (typeof error === 'object' && error !== null) {
    transactionFinalizerFailures.add(error)
  }
  return error
}

// Only a failure before any statement ran can be retried: past BEGIN the statements may
// have run, so their failure stands.
function isRetryablePipelinedSetupError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    transactionSetupFailures.has(error) &&
    isRetryableTransactionSetupError(error)
  )
}

/**
 * Runs `beginStatement`, `statements` and COMMIT on `client` as one BatchQuery: written at once
 * and ended by a single Sync, so the transaction costs one round trip, and PostgreSQL runs
 * nothing behind a statement that fails, BEGIN included. A failure before BEGIN replied is a
 * setup failure (nothing ran, so the caller may retry on another client), and a failed COMMIT
 * a finalizer failure. A failed statement leaves the transaction aborted, COMMIT skipped: it is
 * rolled back here so the client comes back idle, and the statement's error is thrown.
 */
async function runPgTransaction(
  client: PoolClient,
  beginStatement: string,
  statements: DatabaseStatement[],
  signal: AbortSignal | undefined
): Promise<QueryResult[]> {
  if (signal?.aborted) {
    throw createAbortError()
  }

  const batch = new BatchQuery(statements, beginStatement, 'COMMIT')
  client.query(batch)
  const [outcome] = await settle([batch.promise], client, signal)
  if (outcome.status === 'fulfilled') {
    return outcome.value
  }

  const error = outcome.reason
  if (!batch.beganTransaction) {
    throw markTransactionSetupFailure(error)
  }
  if (batch.reachedEnd) {
    throw markTransactionFinalizerFailure(error)
  }
  if (!isConnectionStateError(error)) {
    try {
      await client.query('ROLLBACK')
    } catch (rollbackError) {
      // The client's state is uncertain: discard it, and report the statement's error.
      markClientDisposable(rollbackError)
      throw markTransactionFinalizerFailure(error)
    }
  }
  throw error
}

// Waits for every query, unless `signal` aborts first, and marks the client for disposal
// when one of them failed on the connection itself.
async function settle<T>(
  pending: Promise<T>[],
  client: PoolClient,
  signal: AbortSignal | undefined
): Promise<PromiseSettledResult<T>[]> {
  const settling = Promise.allSettled(pending)
  const outcomes = signal ? await settleUnlessAborted(client, settling, signal) : await settling

  for (const outcome of outcomes) {
    if (outcome.status === 'rejected' && isConnectionStateError(outcome.reason)) {
      markClientDisposable(outcome.reason)
    }
  }

  return outcomes
}

// Like runPgQuery: an abort cancels what the backend is running and rejects at once, and
// the client is then discarded with the AbortError.
async function settleUnlessAborted<T>(
  client: PoolClient,
  settling: Promise<T>,
  signal: AbortSignal
): Promise<T> {
  assertValidSignal(signal)

  let onAbort: (() => void) | undefined
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => {
      void cancelQuery(client).catch(() => undefined)
      reject(createAbortError())
    }
    signal.addEventListener('abort', onAbort, { once: true })
  })

  try {
    return await Promise.race([settling, aborted])
  } finally {
    if (onAbort) {
      signal.removeEventListener('abort', onAbort)
    }
  }
}

function getPendingSettingsSql(
  statementTimeoutMs: number | undefined,
  pendingSearchPath: string | undefined
): string {
  if (statementTimeoutMs && pendingSearchPath) {
    return "SELECT set_config('statement_timeout', $1, true), set_config('search_path', $2, true) OFFSET 1"
  }

  if (statementTimeoutMs) {
    return "SELECT set_config('statement_timeout', $1, true) OFFSET 1"
  }

  return "SELECT set_config('search_path', $1, true) OFFSET 1"
}

function withDefaultTransactionSettings(
  options: TransactionOptions | undefined,
  isExternalPool: boolean | undefined
): PgBeginTransactionOptions | undefined {
  if (!isExternalPool) {
    return withDefaultStatementTimeout(options)
  }

  if (!options) {
    return defaultExternalPoolBeginTransactionOptions
  }

  return {
    ...options,
    statementTimeoutMs: options.timeout === undefined ? defaultStatementTimeoutMs : undefined,
    searchPath: externalPoolSearchPath,
  }
}

// PgPoolExecutor already derives the deferred statement_timeout from options.timeout,
// so options only need rewriting when the global default has to be injected.
function withDefaultStatementTimeout(
  options: TransactionOptions | undefined
): PgBeginTransactionOptions | undefined {
  if (!defaultBeginTransactionOptions || options?.timeout !== undefined) {
    return options
  }

  if (!options) {
    return defaultBeginTransactionOptions
  }

  return { ...options, statementTimeoutMs: defaultStatementTimeoutMs }
}

function createDisposedTenantConnectionError(): Error {
  return new Error('Cannot use a disposed PgTenantConnection')
}

function ensureError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

async function runPgQuery<T extends QueryResultRow = QueryResultRow>(
  client: PoolClient,
  statement: string | DatabaseStatement,
  options?: DatabaseQueryArgument
): Promise<QueryResult<T>> {
  const signal = Array.isArray(options) ? undefined : options?.signal
  const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)

  if (!signal) {
    try {
      return await client.query<T>(query.text, query.values)
    } catch (e) {
      if (isConnectionStateError(e)) {
        markClientDisposable(e)
      }
      throw e
    }
  }

  assertValidSignal(signal)

  let aborted = false
  let cancelPromise: Promise<void> | undefined
  let rejectAbort: ((error: Error) => void) | undefined
  const abortPromise = new Promise<never>((_, reject) => {
    rejectAbort = reject
  })

  const onAbort = () => {
    aborted = true
    cancelPromise = cancelQuery(client).catch(() => undefined)
    rejectAbort?.(createAbortError())
    rejectAbort = undefined
  }

  signal.addEventListener('abort', onAbort, { once: true })

  try {
    const queryPromise = client.query<T>(query.text, query.values)
    const result = await Promise.race([queryPromise, abortPromise])

    if (aborted) {
      throw createAbortError()
    }

    return result
  } catch (e) {
    if (aborted) {
      void cancelPromise
      throw createAbortError()
    }

    if (isConnectionStateError(e)) {
      markClientDisposable(e)
    }
    throw e
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}

function getQuerySignal(options?: DatabaseQueryArgument): AbortSignal | undefined {
  return Array.isArray(options) ? undefined : options?.signal
}
