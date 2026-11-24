import { ERRORS } from '@internal/errors'
import { DatabaseError, type FieldDef, QueryResult, QueryResultRow, types } from 'pg'
import {
  type BatchStatement,
  type DatabaseErrorResponse,
  type DatabasePoolTarget,
  POSTGRES_ERROR_FIELDS,
} from '../../../applications/database/protocol'
import { getConfig } from '../../../config'
import {
  DatabaseExecutor,
  DatabaseQueryArgument,
  DatabaseStatement,
  DatabaseTransaction,
  DatabaseTransactionalExecutor,
  ScopedStatement,
  TenantConnection,
  TransactionOptions,
} from '../connection'
import { searchPath, TenantConnectionOptions } from '../pool'
import { createAbortError } from '../postgres/pool-errors'
import { buildScopeStatement } from '../postgres/scope'
import {
  normalizeIsolationLevel,
  normalizeStatement,
  normalizeStatementTimeoutMs,
} from '../postgres/sql'
import { installPostgresTypeParsers } from '../postgres/type-parsers'
import {
  DatabaseWattQueryResponse,
  DatabaseWattResponseError,
  DatabaseWattTransport,
  databaseWattClient,
} from './client'

// The Database application returns rows as raw text: they are parsed here, with the same
// pg type parsers a direct pool uses.
installPostgresTypeParsers()

const { databaseStatementTimeout } = getConfig()
const defaultStatementTimeoutMs = normalizeStatementTimeoutMs(databaseStatementTimeout)
const externalPoolSearchPath = searchPath.join(',')

interface WattTransactionSettings {
  statementTimeoutMs?: number
  searchPath?: string
}

interface WattTransactionStart {
  destination: DatabasePoolTarget
  isolationLevel?: string
  readOnly?: boolean
}

/**
 * A transaction on a connection pinned by the Database application.
 *
 * It starts lazily: BEGIN, the deferred settings, the scope statement and the first query
 * travel in one `database.batch` message instead of one message each. A transaction that
 * never runs a query sends nothing at all.
 */
class WattPgTransaction implements DatabaseTransaction {
  private wattCompleted = false
  private pendingStatementTimeoutMs?: number
  private pendingSearchPath?: string
  private lockId?: string
  private starting?: Promise<void>
  private readonly deferredStatements: BatchStatement[] = []

  constructor(
    private readonly start: WattTransactionStart,
    private readonly operation: (() => string | undefined) | undefined,
    private readonly transport: DatabaseWattTransport,
    settings: WattTransactionSettings = {}
  ) {
    this.pendingStatementTimeoutMs = settings.statementTimeoutMs
    this.pendingSearchPath = settings.searchPath
  }

  isCompleted(): boolean {
    return this.wattCompleted
  }

  isStarted(): boolean {
    return this.lockId !== undefined || this.starting !== undefined
  }

  // Deferred settings are handed to setScope so they ride on the scope statement,
  // exactly like PgTransaction, instead of costing their own round trip.
  takePendingStatementTimeoutMs(): number | undefined {
    const timeoutMs = this.pendingStatementTimeoutMs
    this.pendingStatementTimeoutMs = undefined
    return timeoutMs
  }

  takePendingSearchPath(): string | undefined {
    const pending = this.pendingSearchPath
    this.pendingSearchPath = undefined
    return pending
  }

  // Queues a statement whose result nobody reads (the scope statement) to run right
  // after BEGIN, in the same message as the first query.
  defer(statement: BatchStatement): void {
    this.deferredStatements.push(statement)
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.wattCompleted) {
      throw new Error('Cannot query a completed transaction')
    }

    const signal = Array.isArray(options) ? undefined : options?.signal
    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)

    if (this.lockId === undefined) {
      if (!this.starting) {
        const result = this.startWith<T>(query, signal)
        this.starting = result.then(
          () => undefined,
          () => undefined
        )
        return result
      }

      // A concurrent query waits for the one that is opening the transaction.
      await this.starting
      if (this.wattCompleted || this.lockId === undefined) {
        throw new Error('Cannot query a completed transaction')
      }
    }

    if (this.pendingStatementTimeoutMs !== undefined || this.pendingSearchPath !== undefined) {
      await this.applyPendingSettings(signal)
    }

    return this.lockedQuery<T>(query.text, query.values, signal)
  }

  async commit(): Promise<void> {
    await this.starting
    if (this.wattCompleted) {
      return
    }

    // Nothing ran, so there is no transaction to commit.
    if (this.lockId === undefined) {
      this.wattCompleted = true
      return
    }

    try {
      await withPgErrors(this.transport.commitTransaction({ lockId: this.lockId }))
    } finally {
      this.wattCompleted = true
    }
  }

  async rollback(): Promise<void> {
    await this.starting
    if (this.wattCompleted) {
      return
    }

    if (this.lockId === undefined) {
      this.wattCompleted = true
      return
    }

    try {
      await withPgErrors(this.transport.rollbackTransaction({ lockId: this.lockId }))
    } finally {
      this.wattCompleted = true
    }
  }

  private async startWith<T extends QueryResultRow>(
    query: DatabaseStatement,
    signal: AbortSignal | undefined
  ): Promise<QueryResult<T>> {
    const statements: BatchStatement[] = []
    const settings = takePendingSettingsStatement(this)
    if (settings) {
      statements.push(settings)
    }
    statements.push(...this.deferredStatements, { sql: query.text, values: query.values })
    this.deferredStatements.length = 0

    try {
      const response = await withPgErrors(
        this.transport.batch(
          {
            destination: this.start.destination,
            isolationLevel: this.start.isolationLevel,
            readOnly: this.start.readOnly,
            operationName: this.operation?.(),
            statements,
            commit: false,
          },
          { signal }
        )
      )
      this.lockId = response.lockId
      if (this.lockId === undefined) {
        this.wattCompleted = true
        throw new Error('Database Watt batch response is missing the lock ID')
      }
      return toPgQueryResult<T>(response)
    } catch (error) {
      // A failed batch leaves no transaction behind; an aborted request may have, but
      // its lock is unknown here and expires on the Database application side.
      this.wattCompleted = true
      throw error
    }
  }

  private async applyPendingSettings(signal?: AbortSignal): Promise<void> {
    const statement = takePendingSettingsStatement(this)
    if (statement) {
      await this.lockedQuery(statement.sql, statement.values, signal)
    }
  }

  private async lockedQuery<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    values: unknown[] | undefined,
    signal: AbortSignal | undefined
  ): Promise<QueryResult<T>> {
    try {
      const response = await withPgErrors(
        this.transport.lockedQuery<T>(
          {
            lockId: this.lockId as string,
            operationName: this.operation?.(),
            sql,
            values,
          },
          { signal }
        )
      )
      return toPgQueryResult<T>(response)
    } catch (error) {
      // The Database application already dropped the pinned connection, so
      // there is nothing left to commit or roll back.
      if (isConnectionDiscarded(error)) {
        this.wattCompleted = true
      }
      throw error
    }
  }
}

function takePendingSettingsStatement(tnx: WattPgTransaction): BatchStatement | undefined {
  const timeoutMs = tnx.takePendingStatementTimeoutMs()
  const pendingSearchPath = tnx.takePendingSearchPath()
  const setters: string[] = []
  const values: string[] = []

  if (timeoutMs) {
    values.push(`${timeoutMs}ms`)
    setters.push(`set_config('statement_timeout', $${values.length}, true)`)
  }
  if (pendingSearchPath) {
    values.push(pendingSearchPath)
    setters.push(`set_config('search_path', $${values.length}, true)`)
  }

  return setters.length > 0 ? { sql: `SELECT ${setters.join(', ')}`, values } : undefined
}

/**
 * Runs exactly one statement as a whole transaction (BEGIN, scope, statement, COMMIT)
 * with a single message. Used by StoragePgDB for operations that issue one query.
 */
class WattPgSingleStatementExecutor implements DatabaseExecutor {
  private used = false

  constructor(
    private readonly start: WattTransactionStart,
    private readonly setup: BatchStatement[],
    private readonly operation: (() => string | undefined) | undefined,
    private readonly transport: DatabaseWattTransport
  ) {}

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.used) {
      throw new Error('A single statement executor runs only one statement')
    }
    this.used = true

    const signal = Array.isArray(options) ? undefined : options?.signal
    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const response = await withPgErrors(
      this.transport.batch(
        {
          destination: this.start.destination,
          isolationLevel: this.start.isolationLevel,
          readOnly: this.start.readOnly,
          operationName: this.operation?.(),
          statements: [...this.setup, { sql: query.text, values: query.values }],
          commit: true,
        },
        { signal }
      )
    )

    return toPgQueryResult<T>(response)
  }
}

export class WattPgExecutor implements DatabaseTransactionalExecutor {
  constructor(
    private readonly destination: DatabasePoolTarget,
    private readonly operation?: () => string | undefined,
    private readonly transport: DatabaseWattTransport = databaseWattClient
  ) {}

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const signal = Array.isArray(options) ? undefined : options?.signal
    const response = await withPgErrors(
      this.transport.query<T>(
        {
          destination: this.destination,
          operationName: this.operation?.(),
          sql: query.text,
          values: query.values,
        },
        { signal }
      )
    )

    return toPgQueryResult<T>(response)
  }

  async beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    return this.begin(options, {
      statementTimeoutMs: normalizeStatementTimeoutMs(options?.timeout),
    })
  }

  // No message is sent here: the transaction opens with its first query.
  async begin(
    options: TransactionOptions | undefined,
    settings: WattTransactionSettings
  ): Promise<WattPgTransaction> {
    return new WattPgTransaction(
      this.getTransactionStart(options),
      this.operation,
      this.transport,
      settings
    )
  }

  singleStatement(
    options: TransactionOptions | undefined,
    setup: BatchStatement[]
  ): WattPgSingleStatementExecutor {
    return new WattPgSingleStatementExecutor(
      this.getTransactionStart(options),
      setup,
      this.operation,
      this.transport
    )
  }

  /**
   * Runs `statements` as one committed transaction with a single message. Returns the
   * results of the statements flagged with `result`, then the last statement's.
   */
  async batchResults(
    statements: BatchStatement[],
    signal: AbortSignal | undefined
  ): Promise<QueryResult[]> {
    const response = await withPgErrors(
      this.transport.batch(
        {
          ...this.getTransactionStart(undefined),
          operationName: this.operation?.(),
          statements,
          commit: true,
        },
        { signal }
      )
    )

    const results = (response.results ?? []).map((result) => toPgQueryResult(result))
    results.push(toPgQueryResult(response))
    return results
  }

  private getTransactionStart(options: TransactionOptions | undefined): WattTransactionStart {
    return {
      destination: this.destination,
      // Same leniency as the direct path (unknown levels mean plain BEGIN); the
      // Database application only accepts normalized levels on the wire.
      isolationLevel: normalizeIsolationLevel(options?.isolation),
      readOnly: options?.readOnly,
    }
  }
}

export class WattPgTenantConnection implements TenantConnection {
  readonly role: string
  private readonly executor: WattPgExecutor
  private readonly headersPayload: string
  private userPayload?: string
  private wattAbortSignal?: AbortSignal

  constructor(
    private readonly options: TenantConnectionOptions,
    private readonly transport: DatabaseWattTransport,
    headersPayload?: string
  ) {
    this.role = options.user.payload.role || 'anon'
    this.headersPayload = headersPayload ?? JSON.stringify(options.headers || {})
    this.executor = new WattPgExecutor(
      {
        connectionString: options.dbUrl,
        id: options.tenantId,
        isExternalPool: Boolean(options.isExternalPool),
        maxConnections: getDestinationMaxConnections(options),
      },
      options.operation,
      transport
    )
  }

  dispose(): void {}

  setAbortSignal(signal: AbortSignal): void {
    this.wattAbortSignal = signal
  }

  getAbortSignal(): AbortSignal | undefined {
    return this.wattAbortSignal
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    return this.executor.query<T>(statement, mergeSignalOptions(options, this.wattAbortSignal))
  }

  async beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    this.assertNotAborted()
    return this.executor.beginTransaction(options)
  }

  asSuperUser(): TenantConnection {
    const connection = new WattPgTenantConnection(
      {
        ...this.options,
        user: this.options.superUser,
      },
      this.transport,
      this.headersPayload
    )

    if (this.wattAbortSignal) {
      connection.setAbortSignal(this.wattAbortSignal)
    }

    return connection
  }

  // Mirrors PgTenantConnection.transaction(): the caller's timeout wins over
  // DATABASE_STATEMENT_TIMEOUT, and external poolers get search_path per transaction.
  async transaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    this.assertNotAborted()
    return this.executor.begin(options, {
      statementTimeoutMs:
        options?.timeout !== undefined
          ? normalizeStatementTimeoutMs(options.timeout)
          : defaultStatementTimeoutMs,
      searchPath: this.options.isExternalPool ? externalPoolSearchPath : undefined,
    })
  }

  async setScope(tnx: DatabaseExecutor): Promise<void> {
    let statementTimeoutMs: number | undefined
    let pendingSearchPath: string | undefined
    if (tnx instanceof WattPgTransaction) {
      statementTimeoutMs = tnx.takePendingStatementTimeoutMs()
      pendingSearchPath = tnx.takePendingSearchPath()
    }

    const scope = this.buildScope(statementTimeoutMs, pendingSearchPath)

    // Before the transaction has started, the scope rides along with its first query.
    if (tnx instanceof WattPgTransaction && !tnx.isStarted()) {
      tnx.defer({ sql: scope.text, values: scope.values })
      return
    }

    await tnx.query(scope)
  }

  /**
   * An executor that runs one statement as a whole scoped transaction (the same BEGIN,
   * settings and scope as transaction() plus setScope(), then COMMIT) in one message.
   */
  singleStatementExecutor(options?: TransactionOptions): DatabaseExecutor {
    this.assertNotAborted()
    const scope = this.buildScope(
      options?.timeout !== undefined
        ? normalizeStatementTimeoutMs(options.timeout)
        : defaultStatementTimeoutMs,
      this.options.isExternalPool ? externalPoolSearchPath : undefined
    )

    return this.executor.singleStatement(options, [{ sql: scope.text, values: scope.values }])
  }

  /**
   * Runs `statements` as one transaction in one message: BEGIN, the same settings
   * singleStatementExecutor() applies, then each statement after the scope it asks for
   * (this connection's or the super user's), then COMMIT. Scope statements set the same
   * keys, so each one fully replaces the one before it.
   */
  async scopedBatch(statements: ScopedStatement[]): Promise<QueryResult[]> {
    this.assertNotAborted()
    if (statements.length === 0) {
      return []
    }

    const superUser = statements.some((statement) => statement.asSuperUser)
      ? new WattPgTenantConnection(
          { ...this.options, user: this.options.superUser },
          this.transport,
          this.headersPayload
        )
      : undefined

    const batch: BatchStatement[] = []
    const last = statements.length - 1
    let applied: WattPgTenantConnection | undefined
    for (let i = 0; i <= last; i++) {
      const statement = statements[i]
      const scoped = statement.asSuperUser ? (superUser as WattPgTenantConnection) : this
      if (scoped !== applied) {
        // Transaction-wide settings only need to be set once.
        const scope = applied
          ? scoped.buildScope(undefined, undefined)
          : scoped.buildScope(
              defaultStatementTimeoutMs,
              this.options.isExternalPool ? externalPoolSearchPath : undefined
            )
        batch.push({ sql: scope.text, values: scope.values })
        applied = scoped
      }
      // The last statement's result is the response itself.
      batch.push(
        i < last
          ? { sql: statement.text, values: statement.values, result: true }
          : { sql: statement.text, values: statement.values }
      )
    }

    return this.executor.batchResults(batch, this.wattAbortSignal)
  }

  private buildScope(
    statementTimeoutMs: number | undefined,
    pendingSearchPath: string | undefined
  ) {
    return buildScopeStatement(
      this.options,
      this.role,
      this.getUserPayload(),
      this.headersPayload,
      statementTimeoutMs,
      pendingSearchPath
    )
  }

  private getUserPayload(): string {
    this.userPayload ??= JSON.stringify(this.options.user.payload)
    return this.userPayload
  }

  private assertNotAborted(): void {
    if (this.wattAbortSignal?.aborted) {
      throw createAbortError()
    }
  }
}

export async function getWattPostgresConnection(
  options: TenantConnectionOptions,
  transport: DatabaseWattTransport = databaseWattClient
): Promise<TenantConnection> {
  return new WattPgTenantConnection(options, transport)
}

// The Database application owns one pool per destination for the whole pod, so the
// tenant's connection budget is split across pods only (direct pools also divide by
// the number of storage workers, see PgPoolStrategy.getSettings).
function getDestinationMaxConnections(options: TenantConnectionOptions): number {
  const clusterSize = Math.max(options.clusterSize || 0, 1)
  return Math.max(Math.ceil(options.maxConnections / clusterSize), 1)
}

function mergeSignalOptions(
  options: DatabaseQueryArgument | undefined,
  signal: AbortSignal | undefined
): DatabaseQueryArgument | undefined {
  if (!signal || Array.isArray(options)) {
    return options
  }

  return {
    ...options,
    signal: options?.signal || signal,
  }
}

const TIMEOUT_CODES = new Set([
  'ACQUIRE_TIMEOUT',
  'BUSY',
  'CLIENT_TIMEOUT',
  'CONNECTION_TIMEOUT',
  'MESSAGING_TIMEOUT',
])

function toDatabaseError(response: DatabaseErrorResponse): Error {
  if (response.code === 'POSTGRES_ERROR') {
    const error = new DatabaseError(response.message, 0, 'error')
    error.code = response.sqlState
    for (const field of POSTGRES_ERROR_FIELDS) {
      const value = response.postgres?.[field]
      if (value !== undefined) {
        error[field] = value
      }
    }
    return withDiscardedFlag(error, response)
  }

  if (response.code === 'SERVER_TIMEOUT') {
    const error = new DatabaseError(response.message, 0, 'error')
    error.code = '57014'
    return withDiscardedFlag(error, response)
  }

  // Same client-facing error as a direct pool that cannot hand out a connection
  // in time (PgPoolExecutor maps connect timeouts to DatabaseTimeout).
  if (TIMEOUT_CODES.has(response.code)) {
    return ERRORS.DatabaseTimeout(new DatabaseWattRemoteError(response))
  }

  return new DatabaseWattRemoteError(response)
}

const connectionDiscarded = Symbol('databaseWatt.connectionDiscarded')

function withDiscardedFlag<T extends Error>(error: T, response: DatabaseErrorResponse): T {
  if (response.connectionDiscarded) {
    Object.defineProperty(error, connectionDiscarded, { value: true })
  }
  return error
}

function isConnectionDiscarded(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  if ((error as Error & { [connectionDiscarded]?: true })[connectionDiscarded]) {
    return true
  }

  return error instanceof DatabaseWattRemoteError && error.connectionDiscarded
}

class DatabaseWattRemoteError extends Error {
  readonly code: string
  readonly databaseCode: string
  readonly connectionDiscarded: boolean

  constructor(readonly databaseResponse: DatabaseErrorResponse) {
    super(databaseResponse.message)
    this.name = 'DatabaseWattRemoteError'
    this.code = databaseResponse.code
    this.databaseCode = databaseResponse.code
    this.connectionDiscarded = Boolean(databaseResponse.connectionDiscarded)
  }
}

// Builds row objects from the raw text values the Database application returns, with the
// same type parsers and the same rules (later duplicate column names win) as pg itself.
function toPgQueryResult<T extends QueryResultRow = QueryResultRow>(
  response: DatabaseWattQueryResponse<unknown>
): QueryResult<T> {
  if (!('fields' in response) || !response.fields) {
    return {
      command: '',
      fields: [],
      oid: 0,
      rowCount: response.rowCount,
      rows: response.rows as T[],
    }
  }

  const { fields } = response
  const columns = fields.length
  const parsers = new Array<(value: string) => unknown>(columns)
  for (let i = 0; i < columns; i++) {
    parsers[i] = types.getTypeParser(fields[i].dataTypeID, 'text')
  }

  const rawRows = response.rows
  const rows = new Array<T>(rawRows.length)
  for (let r = 0; r < rawRows.length; r++) {
    const raw = rawRows[r]
    const row: Record<string, unknown> = {}
    for (let i = 0; i < columns; i++) {
      const value = raw[i]
      row[fields[i].name] = value === null ? null : parsers[i](value)
    }
    rows[r] = row as T
  }

  return {
    command: '',
    fields: fields as FieldDef[],
    oid: 0,
    rowCount: response.rowCount,
    rows,
  }
}

async function withPgErrors<Result>(request: Promise<Result>): Promise<Result> {
  try {
    return await request
  } catch (error) {
    if (error instanceof DatabaseWattResponseError) {
      throw toDatabaseError(error.response)
    }

    throw error
  }
}
