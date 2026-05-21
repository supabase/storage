import { TenantConnection } from '../../internal/database/connection'
import { dbQueryPerformance } from '../../internal/monitoring/metrics'
import { escapeLike, StorageKnexDB } from './knex'

class TestStorageKnexDB extends StorageKnexDB {
  runTestQuery() {
    return this.runQuery('TestQuery', async () => 'allowed')
  }
}

describe('escapeLike', () => {
  test('escapes SQL wildcard characters', () => {
    expect(escapeLike('%_abc')).toBe('\\%\\_abc')
    expect(escapeLike('a%b_c')).toBe('a\\%b\\_c')
    expect(escapeLike('plain-text')).toBe('plain-text')
  })
})

function createStorageKnexTestHarness() {
  const transaction = {
    once: vi.fn(),
    commit: vi.fn().mockResolvedValue(undefined),
    rollback: vi.fn().mockResolvedValue(undefined),
  }

  const connection = {
    role: 'anon',
    transactionProvider: vi.fn().mockReturnValue(async () => transaction),
    setScope: vi.fn().mockResolvedValue(undefined),
    getAbortSignal: vi.fn().mockReturnValue(undefined),
  } as unknown as TenantConnection

  const db = new TestStorageKnexDB(connection, {
    tenantId: 'test-tenant',
    host: 'localhost',
  })

  return { db, connection, transaction }
}

describe('StorageKnexDB', () => {
  it('returns the callback result after rolling back the transaction', async () => {
    const { db, connection, transaction } = createStorageKnexTestHarness()

    const result = await db.testPermission(async (txDb) => {
      expect(txDb).toBeInstanceOf(StorageKnexDB)
      expect(txDb).not.toBe(db)
      return 'allowed'
    })

    expect(result).toBe('allowed')
    expect(connection.transactionProvider).toHaveBeenCalledWith(undefined, undefined)
    expect(connection.setScope).toHaveBeenCalledWith(transaction)
    expect(transaction.rollback).toHaveBeenCalledTimes(1)
    expect(transaction.commit).not.toHaveBeenCalled()
  })

  it('rethrows callback failures without wrapping them', async () => {
    const { db, transaction } = createStorageKnexTestHarness()
    const error = new Error('permission denied')

    await expect(
      db.testPermission(async () => {
        throw error
      })
    ).rejects.toBe(error)

    expect(transaction.rollback).toHaveBeenCalledTimes(1)
    expect(transaction.commit).not.toHaveBeenCalled()
  })

  it('records query performance attributes without tenant id labels', async () => {
    const { db } = createStorageKnexTestHarness()
    const recordSpy = vi.spyOn(dbQueryPerformance, 'record')

    try {
      await db.runTestQuery()

      expect(recordSpy).toHaveBeenCalledWith(expect.any(Number), {
        name: 'TestQuery',
      })
    } finally {
      recordSpy.mockRestore()
    }
  })
})
