import { type DatabaseTransaction, quoteIdentifier } from '@internal/database'

// Savepoints opened so far in each transaction. Entries go away with the transaction object.
const savepointsOpened = new WeakMap<DatabaseTransaction, number>()

/**
 * Returns a savepoint name that no earlier savepoint in the same transaction has used.
 *
 * The name ends in a per-transaction counter (`<prefix>_1`, `<prefix>_2`, ...) instead of a
 * random UUID. With `pg_stat_statements.track_utility` on, the savepoint name is part of the
 * normalized statement text, so a random name added two never-repeated entries per nested
 * transaction and evicted application queries. A counter keeps names unique within the
 * transaction, as a UUID did, while repeating across transactions.
 */
export function nextSavepointName(tnx: DatabaseTransaction, prefix: string): string {
  const count = (savepointsOpened.get(tnx) ?? 0) + 1
  savepointsOpened.set(tnx, count)
  return quoteIdentifier(`${prefix}_${count}`)
}
