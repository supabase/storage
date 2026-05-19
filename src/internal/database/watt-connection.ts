import { randomUUID } from 'node:crypto'
import { DatabaseError, QueryResult, QueryResultRow } from 'pg'
import { TransactionOptions } from '@storage/database'
import {
  PgQueryOptions,
  PgStatement,
  PgExecutor,
  PgTenantConnection,
  PgTenantConnectionLike,
  PgTransaction,
  PgTransactionLike,
  PgTransactionalExecutor,
} from './pg-connection'
import { searchPath, TenantConnectionOptions } from './pool'

type PlatformaticGlobal = {
  messaging?: {
    send(application: string, message: string, data: unknown): Promise<unknown>
  }
}

type DatabaseErrorResponse = {
  code: string
  message: string
  requestId?: string
  operationName?: string
  destination?: string
  lockId?: string
  sqlState?: string
  stack?: string
  connectionDiscarded?: boolean
}

type QueryResponse<T extends QueryResultRow = QueryResultRow> = {
  rows: T[]
  rowCount: number
}

type PgQueryArgument = PgQueryOptions | unknown[]

const databaseApplicationId = 'database'

export function hasWattMessaging(): boolean {
  const platformatic = (globalThis as typeof globalThis & { platformatic?: PlatformaticGlobal })
    .platformatic
  return Boolean(platformatic?.messaging)
}

export async function getWattPostgresConnection(
  options: TenantConnectionOptions
): Promise<PgTenantConnection> {
  return new WattPgTenantConnection(options) as unknown as PgTenantConnection
}

class WattPgTenantConnection implements PgTenantConnectionLike {
  readonly role: string
  private readonly executor: DatabaseWattPgExecutor
  private wattAbortSignal?: AbortSignal

  constructor(private readonly options: TenantConnectionOptions) {
    this.role = options.user.payload.role || 'anon'
    this.executor = new DatabaseWattPgExecutor(options.tenantId, options.operation)
  }

  dispose(): Promise<void> {
    return Promise.resolve()
  }

  setAbortSignal(signal: AbortSignal): void {
    this.wattAbortSignal = signal
  }

  getAbortSignal(): AbortSignal | undefined {
    return this.wattAbortSignal
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    return this.executor.query<T>(statement, mergeSignalOptions(options, this.wattAbortSignal))
  }

  async beginTransaction(options?: TransactionOptions): Promise<PgTransactionLike> {
    return this.executor.beginTransaction(options)
  }

  asSuperUser(): PgTenantConnection {
    const connection = new WattPgTenantConnection({
      ...this.options,
      user: this.options.superUser,
    })

    if (this.wattAbortSignal) {
      connection.setAbortSignal(this.wattAbortSignal)
    }

    return connection as unknown as PgTenantConnection
  }

  async transaction(opts?: TransactionOptions): Promise<PgTransaction> {
    return this.beginTransaction(opts) as Promise<PgTransaction>
  }

  async setScope(tnx: PgExecutor): Promise<void> {
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

export class DatabaseWattPgExecutor implements PgTransactionalExecutor {
  private readonly destination: string
  private readonly operation?: () => string | undefined

  constructor(destination: string, operation?: () => string | undefined) {
    this.destination = destination
    this.operation = operation
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const signal = Array.isArray(options) ? undefined : options?.signal
    const response = await sendDatabaseMessage<QueryResponse<T>>(
      'database.query',
      {
        destination: this.destination,
        operationName: this.operation?.(),
        sql: query.text,
        values: query.values,
      },
      signal
    )

    return toPgQueryResult(response)
  }

  async beginTransaction(options?: TransactionOptions): Promise<PgTransaction> {
    const response = await sendDatabaseMessage<{ lockId: string }>('database.beginTransaction', {
      destination: this.destination,
      isolationLevel: options?.isolation,
      operationName: this.operation?.(),
      readOnly: options?.readOnly,
    })

    return new WattPgTransaction(response.lockId, this.operation) as unknown as PgTransaction
  }
}

class WattPgTransaction implements PgTransactionLike {
  private wattCompleted = false
  private readonly lockId: string
  private readonly operation?: () => string | undefined

  constructor(
    lockId: string,
    operation?: () => string | undefined
  ) {
    this.lockId = lockId
    this.operation = operation
  }

  isCompleted(): boolean {
    return this.wattCompleted
  }

  async query<T extends QueryResultRow = QueryResultRow>(
    statement: string | PgStatement,
    options?: PgQueryArgument
  ): Promise<QueryResult<T>> {
    if (this.wattCompleted) {
      throw new Error('Cannot query a completed transaction')
    }

    const query = normalizeStatement(statement, Array.isArray(options) ? options : undefined)
    const signal = Array.isArray(options) ? undefined : options?.signal
    const response = await sendDatabaseMessage<QueryResponse<T>>(
      'database.lockedQuery',
      {
        lockId: this.lockId,
        operationName: this.operation?.(),
        sql: query.text,
        values: query.values,
      },
      signal,
      this.lockId
    )

    return toPgQueryResult(response)
  }

  async commit(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await sendDatabaseMessage('database.commitTransaction', { lockId: this.lockId })
    } finally {
      this.wattCompleted = true
    }
  }

  async rollback(): Promise<void> {
    if (this.wattCompleted) {
      return
    }

    try {
      await sendDatabaseMessage('database.rollbackTransaction', { lockId: this.lockId })
    } finally {
      this.wattCompleted = true
    }
  }
}

function normalizeStatement(statement: string | PgStatement, values?: unknown[]): PgStatement {
  if (typeof statement === 'string') {
    return { text: statement, values }
  }

  return statement
}

function mergeSignalOptions(
  options: PgQueryArgument | undefined,
  signal: AbortSignal | undefined
): PgQueryArgument | undefined {
  if (!signal || Array.isArray(options)) {
    return options
  }

  return {
    ...options,
    signal: options?.signal || signal,
  }
}

async function sendDatabaseMessage<T>(
  message: string,
  data: Record<string, unknown>,
  signal?: AbortSignal,
  lockId?: string
): Promise<T> {
  assertValidSignal(signal)

  const requestId = typeof data.requestId === 'string' ? data.requestId : randomUUID()
  const payload = { ...data, requestId }
  const request = getMessaging().send(databaseApplicationId, message, payload)

  if (!signal) {
    const response = await request
    return parseDatabaseMessageResponse<T>(response)
  }

  let abortListener: (() => void) | undefined
  let settled = false

  const abortPromise = new Promise<never>((_, reject) => {
    abortListener = () => {
      if (settled) {
        return
      }

      void getMessaging().send(databaseApplicationId, 'database.cancel', { requestId, lockId }).catch(() => undefined)
      reject(createAbortError())
    }

    signal.addEventListener('abort', abortListener, { once: true })
  })

  try {
    const response = await Promise.race([request, abortPromise])
    return parseDatabaseMessageResponse<T>(response)
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

function getMessaging(): NonNullable<PlatformaticGlobal['messaging']> {
  const platformatic = (globalThis as typeof globalThis & { platformatic?: PlatformaticGlobal })
    .platformatic
  const messaging = platformatic?.messaging

  if (!messaging) {
    throw new Error('Database Watt messaging API is not available')
  }

  return messaging
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
    error.code = response.code === 'SERVER_TIMEOUT' ? '57014' : undefined
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
