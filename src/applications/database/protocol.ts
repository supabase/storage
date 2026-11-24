export const DATABASE_APPLICATION_ID = 'database'

export const DATABASE_MESSAGES = {
  acquire: 'database.acquire',
  batch: 'database.batch',
  beginTransaction: 'database.beginTransaction',
  cancel: 'database.cancel',
  commitTransaction: 'database.commitTransaction',
  lockedQuery: 'database.lockedQuery',
  query: 'database.query',
  release: 'database.release',
  rollbackTransaction: 'database.rollbackTransaction',
} as const

export type DatabaseErrorCode =
  | 'POSTGRES_ERROR'
  | 'DESTINATION_UNKNOWN'
  | 'CLIENT_TIMEOUT'
  | 'SERVER_TIMEOUT'
  | 'CONNECTION_TIMEOUT'
  | 'ACQUIRE_TIMEOUT'
  | 'MESSAGING_TIMEOUT'
  | 'MESSAGING_ERROR'
  | 'BUSY'
  | 'RESULT_TOO_LARGE'
  | 'PROTOCOL_ERROR'
  | 'SHUTDOWN'

// Fields of pg's DatabaseError that Storage reads when mapping Postgres errors
// (for example `constraint`/`detail` for 23503, `where`/`internalQuery` for 42P01).
export const POSTGRES_ERROR_FIELDS = [
  'severity',
  'detail',
  'hint',
  'position',
  'internalPosition',
  'internalQuery',
  'where',
  'schema',
  'table',
  'column',
  'dataType',
  'constraint',
  'routine',
] as const

export type PostgresErrorField = (typeof POSTGRES_ERROR_FIELDS)[number]

export type PostgresErrorFields = Partial<Record<PostgresErrorField, string>>

export type DatabaseErrorResponse = {
  code: DatabaseErrorCode
  message: string
  requestId?: string
  operationName?: string
  destination?: string
  lockId?: string
  sqlState?: string
  postgres?: PostgresErrorFields
  connectionDiscarded?: boolean
}

export type WireRequestMeta = {
  requestId?: string
  operationName?: string
}

export type DatabasePoolTarget = {
  connectionString: string
  id: string
  isExternalPool: boolean
  maxConnections: number
}

export type QueryRequest = WireRequestMeta & {
  destination: DatabasePoolTarget
  sql: string
  values?: unknown[]
}

export type AcquireConnectionRequest = WireRequestMeta & {
  destination: DatabasePoolTarget
}

export type AcquireConnectionResponse = {
  lockId: string
}

export type LockedQueryRequest = WireRequestMeta & {
  lockId: string
  sql: string
  values?: unknown[]
}

export type ReleaseConnectionRequest = WireRequestMeta & {
  lockId: string
}

export type BeginTransactionRequest = WireRequestMeta & {
  destination: DatabasePoolTarget
  isolationLevel?: string
  readOnly?: boolean
}

export type CommitTransactionRequest = WireRequestMeta & {
  lockId: string
}

export type RollbackTransactionRequest = WireRequestMeta & {
  lockId: string
}

export type CancelRequest = {
  requestId: string
  lockId?: string
}

export type QueryResponse<T = unknown> = {
  rows: T[]
  rowCount: number
}

export type QueryField = {
  name: string
  dataTypeID: number
}

/**
 * Rows exactly as PostgreSQL sent them: one array of text values (or null) per row, in
 * column order. The storage worker that asked for them applies the pg type parsers, so
 * the Database application never builds row objects, dates or JSON values.
 */
export type RawQueryResponse = {
  fields: QueryField[]
  rows: Array<Array<string | null>>
  rowCount: number
}

export type BatchStatement = {
  sql: string
  values?: unknown[]
  /** Also return this statement's result, in the response's `results`. */
  result?: boolean
}

/**
 * Opens a transaction and runs `statements` in it with one message. With `commit` the
 * transaction is committed and its connection released (a whole single-statement
 * transaction); without it the transaction stays open and its lock ID is returned.
 * The response carries the result of the last statement, plus, in `results`, the
 * results of the statements flagged with `result`, in order.
 */
export type BatchRequest = WireRequestMeta & {
  destination: DatabasePoolTarget
  isolationLevel?: string
  readOnly?: boolean
  statements: BatchStatement[]
  commit: boolean
}

export type BatchResponse = RawQueryResponse & {
  lockId?: string
  results?: RawQueryResponse[]
}

export type DatabaseProtocol = {
  [DATABASE_MESSAGES.acquire]: {
    request: AcquireConnectionRequest
    response: AcquireConnectionResponse
  }
  [DATABASE_MESSAGES.batch]: {
    request: BatchRequest
    response: BatchResponse
  }
  [DATABASE_MESSAGES.beginTransaction]: {
    request: BeginTransactionRequest
    response: AcquireConnectionResponse
  }
  [DATABASE_MESSAGES.cancel]: {
    request: CancelRequest
    response: { cancelled: boolean }
  }
  [DATABASE_MESSAGES.commitTransaction]: {
    request: CommitTransactionRequest
    response: { committed: boolean }
  }
  [DATABASE_MESSAGES.lockedQuery]: {
    request: LockedQueryRequest
    response: RawQueryResponse
  }
  [DATABASE_MESSAGES.query]: {
    request: QueryRequest
    response: RawQueryResponse
  }
  [DATABASE_MESSAGES.release]: {
    request: ReleaseConnectionRequest
    response: { released: boolean }
  }
  [DATABASE_MESSAGES.rollbackTransaction]: {
    request: RollbackTransactionRequest
    response: { rolledBack: boolean }
  }
}

export type DatabaseMessageName = keyof DatabaseProtocol

export type DatabaseMessageRequest<Message extends DatabaseMessageName> =
  DatabaseProtocol[Message]['request']

export type DatabaseMessageResponse<Message extends DatabaseMessageName> =
  DatabaseProtocol[Message]['response']
