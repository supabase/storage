import { DatabaseError, QueryResult, QueryResultRow } from 'pg'
import {
  type DatabaseErrorResponse,
  type DatabasePoolTarget,
  type QueryResponse,
} from '../../../applications/database/protocol'
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
import { buildScopeStatement } from '../postgres/scope'
import { normalizeStatement } from '../postgres/sql'
import { DatabaseWattResponseError, DatabaseWattTransport, databaseWattClient } from './client'

class WattPgTransaction implements DatabaseTransaction {
  private wattCompleted = false
  private readonly lockId: string
  private readonly operation?: () => string | undefined

  constructor(
    lockId: string,
    operation: (() => string | undefined) | undefined,
    private readonly transport: DatabaseWattTransport
  ) {
    this.lockId = lockId
    this.operation = operation
  }

  isCompleted(): boolean {
    return this.wattCompleted
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.wattCompleted) {
      throw new Error('Cannot query a completed transaction')
    }

    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const signal = Array.isArray(options) ? undefined : options?.signal
    const response = await withPgErrors(
      this.transport.lockedQuery<T>(
        {
          lockId: this.lockId,
          operationName: this.operation?.(),
          sql: query.text,
          values: query.values,
        },
        { signal }
      )
    )

    return toPgQueryResult(response)
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
}

export class WattPgExecutor implements DatabaseTransactionalExecutor {
  private readonly destination: DatabasePoolTarget
  private readonly operation?: () => string | undefined

  constructor(
    destination: DatabasePoolTarget,
    operation?: () => string | undefined,
    private readonly transport: DatabaseWattTransport = databaseWattClient
  ) {
    this.destination = destination
    this.operation = operation
  }

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
    const response = await withPgErrors(
      this.transport.beginTransaction({
        destination: this.destination,
        isolationLevel: options?.isolation,
        operationName: this.operation?.(),
        readOnly: options?.readOnly,
      })
    )

    return new WattPgTransaction(response.lockId, this.operation, this.transport)
  }
}

export class WattPgTenantConnection implements TenantConnection {
  readonly role: string
  private readonly executor: WattPgExecutor
  private wattAbortSignal?: AbortSignal

  constructor(
    private readonly options: TenantConnectionOptions,
    private readonly transport: DatabaseWattTransport
  ) {
    this.role = options.user.payload.role || 'anon'
    this.executor = new WattPgExecutor(
      {
        connectionString: options.dbUrl,
        id: options.tenantId,
        isExternalPool: Boolean(options.isExternalPool),
        maxConnections: options.maxConnections,
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
    return this.executor.beginTransaction(options)
  }

  asSuperUser(): TenantConnection {
    const connection = new WattPgTenantConnection(
      {
        ...this.options,
        user: this.options.superUser,
      },
      this.transport
    )

    if (this.wattAbortSignal) {
      connection.setAbortSignal(this.wattAbortSignal)
    }

    return connection
  }

  async transaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    return this.beginTransaction(options)
  }

  async setScope(tnx: DatabaseExecutor): Promise<void> {
    const headers = JSON.stringify(this.options.headers || {})
    await tnx.query(
      buildScopeStatement({
        role: this.role,
        jwt: this.options.user.jwt || '',
        subject: this.options.user.payload.sub || '',
        claims: JSON.stringify(this.options.user.payload),
        headers,
        method: this.options.method || '',
        path: this.options.path || '',
        operation: this.options.operation?.() || '',
        searchPath: searchPath.join(','),
      })
    )
  }
}

export async function getWattPostgresConnection(
  options: TenantConnectionOptions,
  transport: DatabaseWattTransport = databaseWattClient
): Promise<TenantConnection> {
  return new WattPgTenantConnection(options, transport)
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

function toDatabaseError(response: DatabaseErrorResponse): Error {
  if (response.code === 'POSTGRES_ERROR') {
    const error = new DatabaseError(response.message, 0, 'error')
    error.code = response.sqlState
    error.stack = response.stack
    return error
  }

  if (
    response.code === 'CLIENT_TIMEOUT' ||
    response.code === 'SERVER_TIMEOUT' ||
    response.code === 'CONNECTION_TIMEOUT' ||
    response.code === 'ACQUIRE_TIMEOUT' ||
    response.code === 'MESSAGING_TIMEOUT'
  ) {
    const error = new DatabaseError(response.message, 0, 'error')
    error.code = response.code === 'SERVER_TIMEOUT' ? '57014' : response.code
    error.stack = response.stack
    return error
  }

  return new DatabaseWattRemoteError(response)
}

class DatabaseWattRemoteError extends Error {
  readonly code: string
  readonly databaseCode: string

  constructor(readonly databaseResponse: DatabaseErrorResponse) {
    super(databaseResponse.message)
    this.name = 'DatabaseWattRemoteError'
    this.code = databaseResponse.code
    this.databaseCode = databaseResponse.code
    this.stack = databaseResponse.stack ?? this.stack
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
