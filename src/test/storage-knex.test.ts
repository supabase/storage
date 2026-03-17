import { StorageKnexDB } from '@storage/database'
import { TenantConnection } from '../internal/database/connection'

function createStorageKnexTestHarness() {
  const transaction = {
    once: jest.fn(),
    commit: jest.fn().mockResolvedValue(undefined),
    rollback: jest.fn().mockResolvedValue(undefined),
  }

  const connection = {
    role: 'anon',
    transactionProvider: jest.fn().mockReturnValue(async () => transaction),
    setScope: jest.fn().mockResolvedValue(undefined),
    getAbortSignal: jest.fn().mockReturnValue(undefined),
  } as unknown as TenantConnection

  const db = new StorageKnexDB(connection, {
    tenantId: 'test-tenant',
    host: 'localhost',
  })

  return { db, connection, transaction }
}

describe('StorageKnexDB.testPermission', () => {
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
})
