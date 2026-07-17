import { randomUUID } from 'node:crypto'
import { getMessaging } from '@platformatic/globals'
import { DatabaseError, QueryResult, QueryResultRow } from 'pg'
import {
  DATABASE_APPLICATION_ID,
  DATABASE_MESSAGES,
  type DatabaseErrorResponse,
  type DatabaseMessageName,
  type DatabaseMessageRequest,
  type DatabaseMessageResponse,
  type QueryResponse,
} from '../../database/protocol'
import type {
  DatabaseExecutor,
  DatabaseQueryArgument,
  DatabaseStatement,
  DatabaseTransaction,
  DatabaseTransactionalExecutor,
  TenantConnection,
  TransactionOptions,
} from './connection'
import { PgTenantConnection } from './pg-connection'
import { searchPath, TenantConnectionOptions } from './pool'

export async function getWattPostgresConnection(
  options: TenantConnectionOptions
): Promise<PgTenantConnection> {
  return new WattPgTenantConnection(options) as unknown as PgTenantConnection
}

class WattPgTenantConnection implements TenantConnection {
  readonly role: string
  private readonly executor: DatabaseWattPgExecutor
  private wattAbortSignal?: AbortSignal

  constructor(private readonly options: TenantConnectionOptions) {
    this.role = options.user.payload.role || 'anon'
    this.executor = new DatabaseWattPgExecutor(options.tenantId, options.operation)
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
    const connection = new WattPgTenantConnection({
      ...this.options,
      user: this.options.superUser,
    })

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
    await tnx.query({
      text: `
        SELECT
          set_config('role', $1, true),
          set_config('request.jwt.claim.role', $2, true),
          set_config('request.jwt', $3, true),
          set_config('request.jwt.claim.sub', $4, true),
          set_config('request.jwt.claims', $5, true),
          set_config('request.headers', $6, true),
          set_config('request.method', $7, true),
          set_config('request.path', $8, true),
          set_config('storage.operation', $9, true),
          set_config('storage.allow_delete_query', 'true', true),
          set_config('search_path', $10, true);
      `,
      values: [
        this.role,
        this.role,
        this.options.user.jwt || '',
        this.options.user.payload.sub || '',
        JSON.stringify(this.options.user.payload),
        headers,
        this.options.method || '',
        this.options.path || '',
        this.options.operation?.() || '',
        searchPath.join(','),
      ],
    })
  }
}

export class DatabaseWattPgExecutor implements DatabaseTransactionalExecutor {
  private readonly destination: string
  private readonly operation?: () => string | undefined

  constructor(destination: string, operation?: () => string | undefined) {
    this.destination = destination
    this.operation = operation
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>> {
    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const signal = Array.isArray(options) ? undefined : options?.signal
    const response = await sendDatabaseMessage(
      DATABASE_MESSAGES.query,
      {
        destination: this.destination,
        operationName: this.operation?.(),
        sql: query.text,
        values: query.values,
      },
      signal
    )

    return toPgQueryResult(response as QueryResponse<T>)
  }

  async beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction> {
    const response = await sendDatabaseMessage(DATABASE_MESSAGES.beginTransaction, {
      destination: this.destination,
      isolationLevel: options?.isolation,
      operationName: this.operation?.(),
      readOnly: options?.readOnly,
    })

    return new WattPgTransaction(response.lockId, this.operation)
  }
}

class WattPgTransaction implements DatabaseTransaction {
  private wattCompleted = false
  private readonly lockId: string
  private readonly operation?: () => string | undefined

  constructor(lockId: string, operation?: () => string | undefined) {
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
    const response = await sendDatabaseMessage(
      DATABASE_MESSAGES.lockedQuery,
      {
        lockId: this.lockId,
        operationName: this.operation?.(),
        sql: query.text,
        values: query.values,
      },
      signal,
      this.lockId
    )

    return toPgQueryResult(response as QueryResponse<T>)
  }

  async commit(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await sendDatabaseMessage(DATABASE_MESSAGES.commitTransaction, { lockId: this.lockId })
    } finally {
      this.wattCompleted = true
    }
  }

  async rollback(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await sendDatabaseMessage(DATABASE_MESSAGES.rollbackTransaction, { lockId: this.lockId })
    } finally {
      this.wattCompleted = true
    }
  }
}

function normalizeStatement(
  statement: string | DatabaseStatement,
  values?: unknown[]
): DatabaseStatement {
  if (typeof statement === 'string') {
    return { text: statement, values }
  }

  return statement
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

async function sendDatabaseMessage<Message extends DatabaseMessageName>(
  message: Message,
  data: DatabaseMessageRequest<Message>,
  signal?: AbortSignal,
  lockId?: string
): Promise<DatabaseMessageResponse<Message>> {
  assertValidSignal(signal)

  const requestId = typeof data.requestId === 'string' ? data.requestId : randomUUID()
  const payload = { ...data, requestId }
  const request = getMessaging().send(DATABASE_APPLICATION_ID, message, payload)

  if (!signal) {
    const response = await request
    return parseDatabaseMessageResponse<DatabaseMessageResponse<Message>>(response)
  }

  let abortListener: (() => void) | undefined
  let settled = false

  const abortPromise = new Promise<never>((_, reject) => {
    abortListener = () => {
      if (settled) {
        return
      }

      void getMessaging()
        .send(DATABASE_APPLICATION_ID, DATABASE_MESSAGES.cancel, { requestId, lockId })
        .catch(() => undefined)
      reject(createAbortError())
    }

    signal.addEventListener('abort', abortListener, { once: true })
  })

  try {
    const response = await Promise.race([request, abortPromise])
    return parseDatabaseMessageResponse<DatabaseMessageResponse<Message>>(response)
  } finally {
    settled = true
    if (abortListener) {
      signal.removeEventListener('abort', abortListener)
    }
  }
}

function parseDatabaseMessageResponse<T>(response: unknown): T {
  if (isDatabaseErrorResponse(response)) {
    throw toDatabaseError(response)
  }

  return response as T
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

function createAbortError(): Error & { code: string } {
  const error = new Error('Query was aborted') as Error & { code: string }
  error.name = 'AbortError'
  error.code = 'ABORT_ERR'
  return error
}

function isDatabaseErrorResponse(response: unknown): response is DatabaseErrorResponse {
  return Boolean(
    response &&
      typeof response === 'object' &&
      typeof (response as { code?: unknown }).code === 'string' &&
      typeof (response as { message?: unknown }).message === 'string'
  )
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

  const error = new Error(response.message) as Error & {
    code?: string
    databaseCode?: string
    databaseResponse?: DatabaseErrorResponse
  }
  error.code = response.code
  error.databaseCode = response.code
  error.databaseResponse = response
  error.stack = response.stack
  return error
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
