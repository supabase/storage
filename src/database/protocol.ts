export const DATABASE_APPLICATION_ID = 'database'

export const DATABASE_MESSAGES = {
  acquire: 'database.acquire',
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

export type DatabaseErrorResponse = {
  code: DatabaseErrorCode
  message: string
  requestId?: string
  operationName?: string
  destination?: string
  lockId?: string
  sqlState?: string
  stack?: string
  connectionDiscarded?: boolean
}

export type WireRequestMeta = {
  requestId?: string
  operationName?: string
}

export type QueryRequest = WireRequestMeta & {
  destination: string
  sql: string
  values?: unknown[]
}

export type AcquireConnectionRequest = WireRequestMeta & {
  destination: string
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
  destination: string
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

export type DatabaseProtocol = {
  [DATABASE_MESSAGES.acquire]: {
    request: AcquireConnectionRequest
    response: AcquireConnectionResponse
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
    response: QueryResponse
  }
  [DATABASE_MESSAGES.query]: {
    request: QueryRequest
    response: QueryResponse
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
