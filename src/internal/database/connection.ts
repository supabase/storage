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

export interface TenantConnection extends DatabaseTransactionalExecutor {
  readonly role: string
  dispose(): void
  setAbortSignal(signal: AbortSignal): void
  getAbortSignal(): AbortSignal | undefined
  asSuperUser(): TenantConnection
  transaction(options?: TransactionOptions): Promise<DatabaseTransaction>
  setScope(transaction: DatabaseExecutor): Promise<void>
  /**
   * Optional: an executor that runs exactly one statement as a whole scoped transaction
   * (what transaction(), setScope() and commit() do around it) more cheaply than the
   * individual steps. Connections that do not provide it take the regular path.
   */
  singleStatementExecutor?(options?: TransactionOptions): DatabaseExecutor
  /**
   * Optional: runs `statements` in one transaction that costs a single round trip, each
   * with this connection's scope or, when flagged `asSuperUser`, the super user's, and
   * returns every statement's result in order. Connections that cannot do it in one
   * round trip do not provide it.
   */
  scopedBatch?(statements: ScopedStatement[]): Promise<QueryResult[]>
}

export interface ScopedStatement extends DatabaseStatement {
  asSuperUser?: boolean
}

export function isDatabaseTransaction(executor: DatabaseExecutor): executor is DatabaseTransaction {
  return (
    'commit' in executor &&
    typeof executor.commit === 'function' &&
    'rollback' in executor &&
    typeof executor.rollback === 'function' &&
    'isCompleted' in executor &&
    typeof executor.isCompleted === 'function'
  )
}

/**
 * Structural check for a tenant connection (direct pg or Database Watt). Use it
 * instead of `instanceof PgTenantConnection` so both adapters take the same path.
 */
export function isTenantConnection(executor: DatabaseExecutor): executor is TenantConnection {
  return (
    'setScope' in executor &&
    typeof executor.setScope === 'function' &&
    'asSuperUser' in executor &&
    typeof executor.asSuperUser === 'function' &&
    'transaction' in executor &&
    typeof executor.transaction === 'function'
  )
}
