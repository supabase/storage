import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { TransactionOptions } from '@storage/database'
import pg, { DatabaseError, Pool, PoolClient, QueryResult, QueryResultRow } from 'pg'
import PgConnection from 'pg/lib/connection'
import { getConfig } from '../../config'
import {
  PoolManager,
  PoolRebalanceOptions,
  PoolStats,
  PoolStrategySettings,
  searchPath,
  TenantConnectionOptions,
} from './pool'
import { getSslSettings } from './ssl'
import {
  createTlsSessionSlot,
  installTlsSessionResumption,
  TlsSessionResumptionClient,
  type TlsSessionSlot,
} from './tls-session-resumption'

const {
  databaseApplicationName,
  databaseConnectionTimeout,
  databaseFreePoolAfterInactivity,
  databaseMaxConnections,
  databasePoolDrainTimeout,
  databaseSSLRootCert,
  databaseStatementTimeout,
  databaseTlsSessionResumption,
} = getConfig()

pg.types.setTypeParser(20, 'text', parseInt)

export interface PgStatement {
  text: string
  values?: unknown[]
}

export interface PgQueryOptions {
  signal?: AbortSignal
}

type PgQueryArgument = PgQueryOptions | unknown[]

interface PgTransactionOptions {
  statementTimeoutMs?: number
}

type PgBeginTransactionOptions = TransactionOptions & PgTransactionOptions

const defaultStatementTimeoutMs = normalizeStatementTimeoutMs(databaseStatementTimeout)

// Shared frozen options for the common transaction() call without caller options.
const defaultBeginTransactionOptions: PgBeginTransactionOptions | undefined =
  defaultStatementTimeoutMs !== undefined
    ? Object.freeze({ statementTimeoutMs: defaultStatementTimeoutMs })
    : undefined

const scopeConfigSetters = `set_config('role', $1, true),
          set_config('request.jwt.claim.role', $2, true),
          set_config('request.jwt', $3, true),
          set_config('request.jwt.claim.sub', $4, true),
          set_config('request.jwt.claims', $5, true),
          set_config('request.headers', $6, true),
          set_config('request.method', $7, true),
          set_config('request.path', $8, true),
          set_config('storage.operation', $9, true),
          set_config('storage.allow_delete_query', 'true', true)`

const scopeConfigSql = `
        SELECT
          ${scopeConfigSetters};
      `

const scopeConfigSqlWithStatementTimeout = `
        SELECT
          ${scopeConfigSetters},
          set_config('statement_timeout', $10, true);
      `

export interface PgExecutor {
  query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>>
}

export interface PgTransactionalExecutor extends PgExecutor {
  beginTransaction(options?: PgBeginTransactionOptions): Promise<PgTransaction>
}

interface PgPoolErrorContext {
  message: string
  tenantId?: string
  project?: string
}

type PgClientWithCancel = PoolClient & {
  processID?: number
  secretKey?: number
  host?: string | string[]
  port?: number
  connectionParameters?: {
    host?: string | string[]
    port?: number
  }
}

const disposeClientOnRelease = Symbol('disposeClientOnRelease')

type DisposableQueryError = Error & {
  [disposeClientOnRelease]?: true
}

const poolDrainCheckIntervalMs = 200
const transactionSetupMaxAttempts = 11
const transactionSetupTotalBudgetMs = 3000
const transactionSetupRetryMinDelayMs = 50
const transactionSetupRetryMaxDelayMs = 200

export type PgCancelConnectionTarget =
  | {
      type: 'socket'
      path: string
    }
  | {
      type: 'tcp'
      host: string
      port: number
    }

export class PgPoolStrategy {
  protected pool?: Pool
  protected tlsSession?: TlsSessionSlot

  constructor(protected readonly options: PoolStrategySettings) {}

  acquire(): PgPoolExecutor {
    return new PgPoolExecutor(this.getPool())
  }

  async destroy(): Promise<void> {
    const originalPool = this.pool

    if (!originalPool) {
      return
    }

    this.pool = undefined
    await this.drainPool(originalPool, 'destroy')
  }

  rebalance(options: PoolRebalanceOptions): void {
    let shouldUpdatePoolMax = false
    const previousMax = this.pool?.options.max

    if (options.clusterSize !== undefined && options.clusterSize !== 0) {
      this.options.clusterSize = options.clusterSize
      shouldUpdatePoolMax = true
    }

    if (options.maxConnections !== undefined) {
      this.options.maxConnections = options.maxConnections
      shouldUpdatePoolMax = true
    }

    if (!shouldUpdatePoolMax) {
      return
    }

    if (this.pool) {
      const nextMax = this.getSettings().maxConnections
      this.pool.options.max = nextMax

      if (previousMax !== undefined && nextMax > previousMax) {
        pulsePgPoolQueue(this.pool)
      }
    }
  }

  getPoolStats(): PoolStats | null {
    if (!this.pool) {
      return null
    }

    return {
      used: this.pool.totalCount - this.pool.idleCount,
      total: this.pool.totalCount,
    }
  }

  protected getPool(): Pool {
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

    return attachPgPoolErrorHandler(
      new Pool({
        min: 0,
        max: settings.maxConnections,
        connectionString: settings.dbUrl,
        connectionTimeoutMillis: databaseConnectionTimeout,
        idleTimeoutMillis: settings.idleTimeoutMillis,
        ssl,
        Client: databaseTlsSessionResumption && ssl ? TlsSessionResumptionClient : undefined,
        application_name: databaseApplicationName,
        options: settings.searchPath
          ? `-c search_path=${settings.searchPath.join(',')}`
          : undefined,
      }),
      {
        message: '[PgPoolStrategy] Idle pg client error',
        tenantId: settings.tenantId,
        project: settings.tenantId,
      }
    )
  }

  private async drainPool(pool: Pool, reason: 'destroy' | 'rebalance'): Promise<void> {
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

  private logPoolDrainTimeout(
    pool: Pool,
    reason: 'destroy' | 'rebalance',
    elapsedMs: number
  ): void {
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

export function attachPgPoolErrorHandler(pool: Pool, context: PgPoolErrorContext): Pool {
  pool.on('error', (error) => {
    logSchema.warning(logger, context.message, {
      type: 'db',
      tenantId: context.tenantId,
      project: context.project,
      error,
    })
  })

  return pool
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

class PgClientErrorTracker {
  private clientError?: Error

  constructor(private readonly client: PoolClient) {
    this.client.on('error', this.onError)
  }

  get error(): Error | undefined {
    return this.clientError
  }

  throwIfErrored(): void {
    if (this.clientError) {
      throw this.clientError
    }
  }

  releaseErrorForQuery(error: unknown): Error | undefined {
    if (this.clientError) {
      return this.clientError
    }

    if (shouldDisposeClient(error)) {
      return ensureError(error)
    }

    return undefined
  }

  releaseErrorForFinalizer(error: unknown): Error {
    return this.clientError ?? ensureError(error)
  }

  detach(): void {
    this.client.off('error', this.onError)
  }

  private readonly onError = (error: unknown): void => {
    const normalizedError = ensureError(error)
    markClientDisposable(normalizedError)

    if (!this.clientError) {
      this.clientError = normalizedError
    }
  }
}

export class PgPoolExecutor implements PgTransactionalExecutor {
  constructor(private readonly pool: Pool) {}

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    assertValidSignal(getQuerySignal(options))

    let client: PoolClient | undefined
    let clientErrorTracker: PgClientErrorTracker | undefined
    let releaseError: Error | undefined

    try {
      client = await this.pool.connect()
      clientErrorTracker = new PgClientErrorTracker(client)
      const result = await runPgQuery<T>(client, statement, options)
      clientErrorTracker.throwIfErrored()
      return result
    } catch (e) {
      if (!client) {
        if (isConnectionTimeoutError(e)) {
          throw ERRORS.DatabaseTimeout(e)
        }

        throw e
      }

      releaseError = clientErrorTracker?.releaseErrorForQuery(e)
      throw releaseError ?? e
    } finally {
      if (client && clientErrorTracker) {
        const trackedError = clientErrorTracker.error
        try {
          client.release(releaseError ?? trackedError)
        } finally {
          clientErrorTracker.detach()
        }
      }
    }
  }

  async beginTransaction(options?: PgBeginTransactionOptions): Promise<PgTransaction> {
    let client: PoolClient
    try {
      client = await this.pool.connect()
    } catch (e) {
      if (isConnectionTimeoutError(e)) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }

    const clientErrorTracker = new PgClientErrorTracker(client)
    const transaction = new PgTransaction(client, clientErrorTracker, {
      statementTimeoutMs: options?.statementTimeoutMs ?? options?.timeout,
    })

    try {
      await transaction.runSetupQuery(buildBeginStatement(options))
      return transaction
    } catch (e) {
      if (!transaction.isCompleted()) {
        const releaseError = clientErrorTracker.releaseErrorForFinalizer(e)
        try {
          client.release(releaseError)
        } finally {
          clientErrorTracker.detach()
        }
      }
      throw e
    }
  }
}

export class PgTransaction implements PgExecutor {
  private completed = false
  private statementTimeoutMs?: number

  constructor(
    private readonly client: PoolClient,
    private readonly clientErrorTracker?: PgClientErrorTracker,
    options: PgTransactionOptions = {}
  ) {
    this.statementTimeoutMs = normalizeStatementTimeoutMs(options.statementTimeoutMs)
  }

  isCompleted(): boolean {
    return this.completed
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    return this.runQuery(statement, options, true)
  }

  async runSetupQuery<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    // Setup statements must not consume a deferred timeout before scope can fold it in.
    return this.runQuery(statement, options, false)
  }

  // Hands the deferred timeout to the caller (setScope) so it can be folded
  // into its next statement instead of costing a separate set_config round trip.
  takePendingStatementTimeoutMs(): number | undefined {
    const timeoutMs = this.statementTimeoutMs
    this.statementTimeoutMs = undefined
    return timeoutMs
  }

  private async runQuery<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options: PgQueryArgument | undefined,
    applyPendingStatementTimeout: boolean
  ): Promise<QueryResult<T>> {
    if (this.completed) {
      throw new Error('Cannot query a completed transaction')
    }

    const signal = getQuerySignal(options)

    try {
      assertValidSignal(signal)

      if (applyPendingStatementTimeout) {
        await this.applyPendingStatementTimeoutBeforeQuery(signal)
      }

      const result = await runPgQuery<T>(this.client, statement, options)
      this.clientErrorTracker?.throwIfErrored()
      return result
    } catch (e) {
      const releaseError =
        this.clientErrorTracker?.releaseErrorForQuery(e) ??
        (shouldDisposeClient(e) ? ensureError(e) : undefined)
      if (releaseError) {
        this.completed = true
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
      this.clientErrorTracker?.throwIfErrored()
    } catch (e) {
      // A failed transaction finalizer leaves cleanup state uncertain, so release
      // with the error even for recoverable SQLSTATEs such as 40001 at COMMIT.
      releaseError = this.clientErrorTracker?.releaseErrorForFinalizer(e) ?? ensureError(e)
      throw e
    } finally {
      this.completed = true
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
      this.clientErrorTracker?.throwIfErrored()
    } catch (e) {
      // A failed transaction finalizer leaves cleanup state uncertain, so release
      // with the error even for recoverable SQLSTATEs.
      releaseError = this.clientErrorTracker?.releaseErrorForFinalizer(e) ?? ensureError(e)
      throw e
    } finally {
      this.completed = true
      this.release(releaseError)
    }
  }

  private release(releaseError?: Error): void {
    const trackedError = this.clientErrorTracker?.error
    try {
      this.client.release(releaseError ?? trackedError)
    } finally {
      this.clientErrorTracker?.detach()
    }
  }

  private async applyPendingStatementTimeoutBeforeQuery(signal?: AbortSignal): Promise<void> {
    const timeoutMs = this.takePendingStatementTimeoutMs()
    if (!timeoutMs) {
      return
    }

    await runPgQuery(
      this.client,
      {
        text: `SELECT set_config('statement_timeout', $1, true)`,
        values: [`${timeoutMs}ms`],
      },
      { signal }
    )
  }
}

export class PgTenantConnection {
  static poolManager = new PgPoolManager()
  public readonly role: string
  private abortSignal?: AbortSignal
  private disposed = false
  private readonly headersPayload: string
  private readonly userPayload: string

  constructor(
    public readonly pool: PgPoolStrategy,
    protected readonly options: TenantConnectionOptions
  ) {
    this.role = options.user.payload.role || 'anon'
    this.headersPayload = JSON.stringify(options.headers || {})
    this.userPayload = JSON.stringify(options.user.payload)
  }

  static stop() {
    return PgTenantConnection.poolManager.destroyAll()
  }

  static async create(options: TenantConnectionOptions) {
    const pgPool = PgTenantConnection.poolManager.getPool(options)
    return new this(pgPool, options)
  }

  dispose() {
    this.disposed = true
    return Promise.resolve()
  }

  setAbortSignal(signal: AbortSignal) {
    this.abortSignal = signal
  }

  getAbortSignal(): AbortSignal | undefined {
    return this.abortSignal
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    this.assertNotDisposed()
    return this.pool.acquire().query<T>(statement, options)
  }

  async beginTransaction(options?: TransactionOptions): Promise<PgTransaction> {
    return this.beginTransactionWithRetry(options)
  }

  asSuperUser() {
    this.assertNotDisposed()

    const tenantConnection = new PgTenantConnection(this.pool, {
      ...this.options,
      user: this.options.superUser,
    })

    if (this.abortSignal) {
      tenantConnection.setAbortSignal(this.abortSignal)
    }

    return tenantConnection
  }

  async transaction(opts?: TransactionOptions): Promise<PgTransaction> {
    this.assertNotDisposed()

    try {
      const beginOptions = withDefaultStatementTimeout(opts)
      const transaction = await this.beginTransactionWithRetry(beginOptions)

      if (this.options.isExternalPool) {
        try {
          await transaction.runSetupQuery({
            text: `SELECT set_config('search_path', $1, true)`,
            values: [searchPath.join(',')],
          })
        } catch (e) {
          await this.rollbackTransactionSafely(transaction, e, 'search_path setup')
          throw e
        }
      }

      return transaction
    } catch (e) {
      if (isConnectionTimeoutError(e)) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }
  }

  private async beginTransactionWithRetry(
    opts?: PgBeginTransactionOptions
  ): Promise<PgTransaction> {
    const startedAt = performance.now()
    let delayMs = transactionSetupRetryMinDelayMs

    for (let attempt = 1; ; attempt++) {
      try {
        return await this.beginTransactionForRequest(opts)
      } catch (e) {
        // Full jitter with exponential backoff: 50-100, 100-200, then 200ms.
        const sleepMs = Math.min(delayMs * (1 + Math.random()), transactionSetupRetryMaxDelayMs)
        const elapsedMs = performance.now() - startedAt

        if (
          !isRetryableTransactionSetupError(e) ||
          attempt >= transactionSetupMaxAttempts ||
          elapsedMs + sleepMs >= transactionSetupTotalBudgetMs
        ) {
          throw e
        }

        await wait(sleepMs)
        delayMs = Math.min(delayMs * 2, transactionSetupRetryMaxDelayMs)
      }
    }
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

  private async rollbackTransactionSafely(
    transaction: PgTransaction,
    originalError: unknown,
    reason: string
  ): Promise<void> {
    try {
      await transaction.rollback()
    } catch (rollbackError) {
      logSchema.warning(logger, '[PgTenantConnection] Failed to rollback transaction', {
        type: 'db',
        tenantId: this.options.tenantId,
        project: this.options.tenantId,
        error: rollbackError,
        metadata: JSON.stringify({
          reason,
          originalError: String(originalError),
        }),
      })
    }
  }

  async setScope(tnx: PgExecutor) {
    const statementTimeoutMs =
      tnx instanceof PgTransaction ? tnx.takePendingStatementTimeoutMs() : undefined

    const values: unknown[] = [
      this.role,
      this.role,
      this.options.user.jwt || '',
      this.options.user.payload.sub || '',
      this.userPayload,
      this.headersPayload,
      this.options.method || '',
      this.options.path || '',
      this.options.operation?.() || '',
    ]

    if (statementTimeoutMs) {
      values.push(`${statementTimeoutMs}ms`)
    }

    await tnx.query({
      text: statementTimeoutMs ? scopeConfigSqlWithStatementTimeout : scopeConfigSql,
      values,
    })
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

function normalizeStatementTimeoutMs(timeoutMs: number | undefined): number | undefined {
  if (timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return undefined
  }

  return timeoutMs
}

function createDisposedTenantConnectionError(): Error {
  return new Error('Cannot use a disposed PgTenantConnection')
}

function ensureError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

export function createAbortError(): Error & { code: string } {
  const error = new Error('Query was aborted') as Error & { code: string }
  error.name = 'AbortError'
  error.code = 'ABORT_ERR'
  return error
}

async function runPgQuery<T extends QueryResultRow = QueryResultRow>(
  client: PoolClient,
  statement: string | PgStatement,
  options?: PgQueryArgument
): Promise<QueryResult<T>> {
  const signal = Array.isArray(options) ? undefined : options?.signal
  assertValidSignal(signal)

  const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
  let aborted = false
  let cancelPromise: Promise<void> | undefined
  let rejectAbort: ((error: Error) => void) | undefined
  const abortPromise = signal
    ? new Promise<never>((_, reject) => {
        rejectAbort = reject
      })
    : undefined

  const rejectWithAbortError = () => {
    rejectAbort?.(createAbortError())
    rejectAbort = undefined
  }

  const onAbort = () => {
    aborted = true
    cancelPromise = cancelPgQuery(client).catch(() => undefined)
    rejectWithAbortError()
  }

  signal?.addEventListener('abort', onAbort, { once: true })

  try {
    const queryPromise = client.query<T>(query.text, query.values)
    const result = await (abortPromise ? Promise.race([queryPromise, abortPromise]) : queryPromise)

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
    signal?.removeEventListener('abort', onAbort)
  }
}

function normalizeStatement(statement: string | PgStatement, values?: unknown[]): PgStatement {
  if (typeof statement === 'string') {
    return { text: statement, values }
  }

  return statement
}

function assertValidSignal(signal?: AbortSignal): void {
  if (!signal) {
    return
  }

  if (!(signal instanceof AbortSignal)) {
    throw new Error('Expected signal to be an instance of AbortSignal')
  }

  if (signal.aborted) {
    throw createAbortError()
  }
}

function getQuerySignal(options?: PgQueryArgument): AbortSignal | undefined {
  return Array.isArray(options) ? undefined : options?.signal
}

function buildBeginStatement(options?: TransactionOptions): string {
  const modes: string[] = []
  const isolationLevel = normalizeIsolationLevel(options?.isolation)

  if (isolationLevel) {
    modes.push(`ISOLATION LEVEL ${isolationLevel}`)
  }

  if (options?.readOnly) {
    modes.push('READ ONLY')
  }

  if (modes.length === 0) {
    return 'BEGIN'
  }

  return `BEGIN ${modes.join(', ')}`
}

function normalizeIsolationLevel(isolation?: string): string | undefined {
  switch (isolation?.toLowerCase()) {
    case 'read committed':
      return 'READ COMMITTED'
    case 'repeatable read':
      return 'REPEATABLE READ'
    case 'serializable':
      return 'SERIALIZABLE'
    default:
      return undefined
  }
}

function isConnectionLimitError(error: unknown): boolean {
  // PgBouncer can report connection limits as 08P01 protocol_violation. That
  // intentionally overlaps isConnectionStateError so these failed clients are
  // retried and disposed instead of being returned to the pool.
  return (
    error instanceof DatabaseError &&
    ((error.code === '08P01' && error.message.includes('no more connections allowed')) ||
      error.message.includes('Max client connections reached'))
  )
}

function isRetryableTransactionSetupError(error: unknown): boolean {
  return (
    isConnectionStateError(error) || isConnectionLimitError(error) || isBrokenClientError(error)
  )
}

// Socket-level of a dead pooled connection can surface as a plain Error.
// No setup is run yet, so a fresh client can safely retry.
// Connection-establishment failures (ECONNREFUSED, connect timeouts) aren't retried.
function isBrokenClientError(error: unknown): boolean {
  if (!(error instanceof Error) || error.name === 'AbortError') {
    return false
  }

  if ((error as DisposableQueryError)[disposeClientOnRelease] === true) {
    return true
  }

  const code = (error as NodeJS.ErrnoException).code
  return (
    code === 'ECONNRESET' ||
    code === 'EPIPE' ||
    error.message === 'Connection terminated unexpectedly' ||
    error.message === 'Client has encountered a connection error and is not queryable'
  )
}

function isConnectionTimeoutError(error: unknown): error is Error {
  return (
    error instanceof Error &&
    (error.message === 'timeout expired' ||
      error.message === 'timeout exceeded when trying to connect' ||
      error.message === 'Connection terminated due to connection timeout')
  )
}

function shouldDisposeClient(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.name === 'AbortError' ||
      (error as DisposableQueryError)[disposeClientOnRelease] === true)
  )
}

function isConnectionStateError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  if (error instanceof DatabaseError) {
    return error.code ? error.code.startsWith('08') : isPgProtocolError(error)
  }

  return isPgProtocolError(error)
}

function isPgProtocolError(error: Error): boolean {
  return (
    error.message.startsWith('received invalid response:') ||
    error.message.startsWith('Received unexpected ') ||
    error.message.startsWith('Unknown authenticationOk message type')
  )
}

function markClientDisposable(error: unknown): void {
  if (error instanceof Error) {
    // PgPoolExecutor.shouldDisposeClient reads this marker from the exact Error instance
    // thrown by runPgQuery. Do not wrap or replace the error before the pool release path.
    const disposableError = error as DisposableQueryError
    disposableError[disposeClientOnRelease] = true
  }
}

async function cancelPgQuery(client: PgClientWithCancel): Promise<void> {
  // PostgreSQL cancel requests are best effort. node-postgres sends them over a
  // fresh raw protocol connection, so SSL-required proxies can close the socket
  // before the backend sees the cancel request.
  const processID = client.processID
  const secretKey = client.secretKey

  if (!processID || !secretKey) {
    return
  }

  const cancelConnection = new PgConnection()
  cancelConnection.unref()
  const target = getPgCancelConnectionTarget(client)

  return new Promise((resolve) => {
    let resolved = false

    const done = () => {
      if (resolved) {
        return
      }

      resolved = true
      clearTimeout(timeout)
      cancelConnection.end()
      resolve()
    }

    const timeout = setTimeout(done, 5000)
    timeout.unref()

    cancelConnection.on('error', done)
    cancelConnection.on('end', done)
    cancelConnection.on('connect', () => {
      try {
        cancelConnection.cancel(processID, secretKey)
      } catch {
        done()
      }
    })

    if (target.type === 'socket') {
      cancelConnection.connect(target.path)
    } else {
      cancelConnection.connect(target.port, target.host)
    }
  })
}

export function getPgCancelConnectionTarget(
  client: Pick<PgClientWithCancel, 'host' | 'port' | 'connectionParameters'>
): PgCancelConnectionTarget {
  const rawHost = client.host || client.connectionParameters?.host || 'localhost'
  const host = Array.isArray(rawHost) ? rawHost[0] || 'localhost' : rawHost
  const port = client.port || client.connectionParameters?.port || 5432

  if (host.startsWith('/')) {
    return {
      type: 'socket',
      path: `${host}/.s.PGSQL.${port}`,
    }
  }

  return {
    type: 'tcp',
    host,
    port,
  }
}
