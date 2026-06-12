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
  isolationLevel?: 'read committed' | 'repeatable read' | 'serializable'
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

export type DestinationConfig = {
  connectionString: string
  id: string
  isExternalPool: boolean
  maxConnections: number
  poolMode?: string | null
}
