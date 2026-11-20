import { ERRORS } from '@internal/errors'
import { DatabaseError, QueryResult, QueryResultRow } from 'pg'
import {
  type DatabaseErrorResponse,
  type DatabasePoolTarget,
  POSTGRES_ERROR_FIELDS,
  type QueryResponse,
} from '../../../applications/database/protocol'
import { getConfig } from '../../../config'
import {
  DatabaseExecutor,
  DatabaseQueryArgument,
  DatabaseStatement,
  DatabaseTransaction,
  DatabaseTransactionalExecutor,
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
import { DatabaseWattResponseError, DatabaseWattTransport, databaseWattClient } from './client'

const { databaseStatementTimeout } = getConfig()
const defaultStatementTimeoutMs = normalizeStatementTimeoutMs(databaseStatementTimeout)
const externalPoolSearchPath = searchPath.join(',')

interface WattTransactionSettings {
  statementTimeoutMs?: number
  searchPath?: string
}

class WattPgTransaction implements DatabaseTransaction {
  private wattCompleted = false
  private pendingStatementTimeoutMs?: number
  private pendingSearchPath?: string

  constructor(
    private readonly lockId: string,
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

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.wattCompleted) {
      throw new Error('Cannot query a completed transaction')
    }

    const signal = Array.isArray(options) ? undefined : options?.signal
    if (this.pendingStatementTimeoutMs !== undefined || this.pendingSearchPath !== undefined) {
      await this.applyPendingSettings(signal)
    }

    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    return this.lockedQuery<T>(query.text, query.values, signal)
  }

  async commit(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await withPgErrors(this.transport.commitTransaction({ lockId: this.lockId }))
    } finally {
      this.wattCompleted = true
    }
  }

  async rollback(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await withPgErrors(this.transport.rollbackTransaction({ lockId: this.lockId }))
    } finally {
      this.wattCompleted = true
    }
  }

  private async applyPendingSettings(signal?: AbortSignal): Promise<void> {
    const timeoutMs = this.takePendingStatementTimeoutMs()
    const pendingSearchPath = this.takePendingSearchPath()
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

    if (setters.length > 0) {
      await this.lockedQuery(`SELECT ${setters.join(', ')}`, values, signal)
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
            lockId: this.lockId,
            operationName: this.operation?.(),
            sql,
            values,
          },
          { signal }
        )
      )
      return toPgQueryResult(response)
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

    return toPgQueryResult(response)
  }

  async beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    return this.begin(options, {
      statementTimeoutMs: normalizeStatementTimeoutMs(options?.timeout),
    })
  }

  async begin(
    options: TransactionOptions | undefined,
    settings: WattTransactionSettings
  ): Promise<WattPgTransaction> {
    const response = await withPgErrors(
      this.transport.beginTransaction({
        destination: this.destination,
        // Same leniency as the direct path (unknown levels mean plain BEGIN); the
        // Database application only accepts normalized levels on the wire.
        isolationLevel: normalizeIsolationLevel(options?.isolation),
        operationName: this.operation?.(),
        readOnly: options?.readOnly,
      })
    )

    return new WattPgTransaction(response.lockId, this.operation, this.transport, settings)
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

    await tnx.query(
      buildScopeStatement(
        this.options,
        this.role,
        this.getUserPayload(),
        this.headersPayload,
        statementTimeoutMs,
        pendingSearchPath
      )
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

function toPgQueryResult<T extends QueryResultRow = QueryResultRow>(
  response: QueryResponse<T>
): QueryResult<T> {
  return {
    command: '',
    fields: [],
    oid: 0,
    rowCount: response.rowCount,
    rows: response.rows,
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
