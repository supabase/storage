import type { QueryResult, QueryResultRow } from 'pg'

/**
 * PostgreSQL-compatible connection contracts shared by the direct pg adapter
 * and the Database Watt transport adapter.
 *
 * Concrete pools, clients, and transport details do not belong in this module.
 */
export interface DatabaseStatement {
  text: string
  values?: unknown[]
}

export interface DatabaseQueryOptions {
  signal?: AbortSignal
}

export type DatabaseQueryArgument = DatabaseQueryOptions | unknown[]

export interface TransactionOptions {
  isolation?: string
  retry?: number
  readOnly?: boolean
  timeout?: number
}

export interface DatabaseExecutor {
  query<T extends QueryResultRow = QueryResultRow>(
    statement: string | DatabaseStatement,
    options?: DatabaseQueryArgument
  ): Promise<QueryResult<T>>
}

export interface DatabaseTransaction extends DatabaseExecutor {
  isCompleted(): boolean
  commit(): Promise<void>
  rollback(): Promise<void>
}

export interface DatabaseTransactionalExecutor extends DatabaseExecutor {
  beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction>
}

export interface TenantConnection extends DatabaseExecutor {
  readonly role: string
  dispose(): void
  setAbortSignal(signal: AbortSignal): void
  getAbortSignal(): AbortSignal | undefined
  beginTransaction(options?: TransactionOptions): Promise<DatabaseTransaction>
  asSuperUser(): TenantConnection
  transaction(options?: TransactionOptions): Promise<DatabaseTransaction>
  setScope(transaction: DatabaseExecutor): Promise<void>
}

// Compatibility names for existing PostgreSQL-facing callers. These aliases
// can be migrated independently without coupling them to the transport split.
export type PgStatement = DatabaseStatement
export type PgQueryOptions = DatabaseQueryOptions
export type PgQueryArgument = DatabaseQueryArgument
export type PgExecutor = DatabaseExecutor
export type PgTransactionLike = DatabaseTransaction
export type PgTenantConnectionLike = TenantConnection
