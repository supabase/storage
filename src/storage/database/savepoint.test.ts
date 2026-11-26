import type { DatabaseTransaction } from '@internal/database'
import { nextSavepointName } from './savepoint'

function transaction(): DatabaseTransaction {
  return {} as DatabaseTransaction
}

describe('nextSavepointName', () => {
  test('numbers savepoints within a transaction instead of using random names', () => {
    const tnx = transaction()

    expect(nextSavepointName(tnx, 'storage_pg_query')).toBe('"storage_pg_query_1"')
    expect(nextSavepointName(tnx, 'storage_pg_query')).toBe('"storage_pg_query_2"')
    expect(nextSavepointName(tnx, 'storage_pg_query')).toBe('"storage_pg_query_3"')
  })

  test('repeats the same names in the next transaction', () => {
    // pg_stat_statements normalizes utility statements by their full text, so names that
    // repeat across transactions keep the number of distinct entries small.
    const first = transaction()
    const second = transaction()
    const names = (tnx: DatabaseTransaction) => [
      nextSavepointName(tnx, 'storage_pg_query'),
      nextSavepointName(tnx, 'storage_pg_query'),
    ]

    expect(names(second)).toEqual(names(first))
  })

  test('never reuses a name within a transaction, even after earlier savepoints end', () => {
    // Postgres resolves RELEASE and ROLLBACK TO to the most recent savepoint with a given name.
    // Keeping names unique per transaction means a late release can only ever target its own
    // savepoint, as it did with random names.
    const tnx = transaction()
    const names = Array.from({ length: 50 }, () => nextSavepointName(tnx, 'storage_pg_query'))

    expect(new Set(names).size).toBe(50)
  })

  test('shares one counter across prefixes in the same transaction', () => {
    const tnx = transaction()

    expect(nextSavepointName(tnx, 'storage_pg_query')).toBe('"storage_pg_query_1"')
    expect(nextSavepointName(tnx, 'iceberg_pg_transaction')).toBe('"iceberg_pg_transaction_2"')
  })
})
