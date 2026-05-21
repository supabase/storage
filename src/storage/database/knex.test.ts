import { TenantConnection } from '../../internal/database/connection'
import { DBMigration } from '../../internal/database/migrations'
import { escapeLike, StorageKnexDB } from './knex'

interface QueryCall {
  method: string
  args: unknown[]
  nested?: QueryCall[]
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

  const db = new StorageKnexDB(connection, {
    tenantId: 'test-tenant',
    host: 'localhost',
  })

  return { db, connection, transaction }
}

function createQueryBuilder(result: unknown) {
  const calls: QueryCall[] = []
  const builder = {
    select: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'select', args })
      return builder
    }),
    orderBy: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'orderBy', args })
      return builder
    }),
    limit: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'limit', args })
      return builder
    }),
    where: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'where', args })
      return builder
    }),
    whereIn: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'whereIn', args })
      return builder
    }),
    whereNull: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'whereNull', args })
      return builder
    }),
    andWhere: vi.fn((...args: unknown[]) => {
      recordNestedCall(calls, 'andWhere', args)
      return builder
    }),
    orWhere: vi.fn((...args: unknown[]) => {
      recordNestedCall(calls, 'orWhere', args)
      return builder
    }),
    update: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'update', args })
      return builder
    }),
    abortOnSignal: vi.fn((...args: unknown[]) => {
      calls.push({ method: 'abortOnSignal', args })
      return Promise.resolve(result)
    }),
  }

  return { builder, calls }
}

function recordNestedCall(calls: QueryCall[], method: string, args: unknown[]) {
  if (typeof args[0] !== 'function') {
    calls.push({ method, args })
    return
  }

  const nested = createQueryBuilder(undefined)
  ;(args[0] as (builder: unknown) => void)(nested.builder)
  calls.push({ method, args: [], nested: nested.calls })
}

function createStorageKnexQueryHarness(result: unknown) {
  const query = createQueryBuilder(result)
  const transaction = {
    from: vi.fn(() => query.builder),
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

  const db = new StorageKnexDB(connection, {
    tenantId: 'test-tenant',
    host: 'localhost',
  })

  return { db, query, transaction }
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

describe('StorageKnexDB migration column normalization', () => {
  class TestStorageKnexDB extends StorageKnexDB {
    normalizeForTest<T extends string | Record<string, unknown>>(columns: T): T {
      return this.normalizeColumns(columns)
    }
  }

  const makeDB = (latestMigration?: keyof typeof DBMigration) =>
    new TestStorageKnexDB({ role: 'anon' } as TenantConnection, {
      tenantId: 'test-tenant',
      host: 'localhost',
      latestMigration,
    })

  it('strips signature columns before the signature migration', () => {
    const db = makeDB('optimize-existing-functions-again')

    expect(
      db.normalizeForTest({
        metadata: {},
        user_metadata: {},
        signature: null,
      })
    ).toEqual({
      metadata: {},
      user_metadata: {},
    })
    expect(db.normalizeForTest('metadata,user_metadata,signature')).toBe('metadata,user_metadata')
  })

  it('keeps signature columns after the signature migration', () => {
    const db = makeDB('add-objects-signature')

    expect(
      db.normalizeForTest({
        metadata: {},
        user_metadata: {},
        signature: null,
      })
    ).toEqual({
      metadata: {},
      user_metadata: {},
      signature: null,
    })
    expect(db.normalizeForTest('metadata,user_metadata,signature')).toBe(
      'metadata,user_metadata,signature'
    )
  })
})

describe('StorageKnexDB object signature methods', () => {
  it('applies lexicographic cursor filters when listing objects for signature generation', async () => {
    const rows = [{ bucket_id: 'bucket-b', name: 'z.txt', version: 'v2' }]
    const { db, query, transaction } = createStorageKnexQueryHarness(rows)

    await expect(
      db.listObjectsForSignatureGeneration({
        bucketId: 'bucket-a',
        objectNames: ['a.txt', 'z.txt'],
        cursor: { bucketId: 'bucket-a', objectName: 'm.txt' },
        force: false,
        limit: 25,
      })
    ).resolves.toBe(rows)

    expect(transaction.from).toHaveBeenCalledWith('objects')
    expect(query.calls).toEqual(
      expect.arrayContaining([
        { method: 'select', args: ['bucket_id', 'name', 'version'] },
        { method: 'orderBy', args: ['bucket_id', 'asc'] },
        { method: 'orderBy', args: ['name', 'asc'] },
        { method: 'limit', args: [25] },
        { method: 'where', args: ['bucket_id', 'bucket-a'] },
        { method: 'whereIn', args: ['name', ['a.txt', 'z.txt']] },
        { method: 'whereNull', args: ['signature'] },
      ])
    )
    expect(query.calls.find((call) => call.method === 'andWhere')?.nested).toEqual([
      { method: 'where', args: ['bucket_id', '>', 'bucket-a'] },
      {
        method: 'orWhere',
        args: [],
        nested: [
          { method: 'where', args: ['bucket_id', 'bucket-a'] },
          { method: 'andWhere', args: ['name', '>', 'm.txt'] },
        ],
      },
    ])
  })

  it('does not filter out already signed objects when force is true', async () => {
    const rows = [{ bucket_id: 'bucket-a', name: 'a.txt', version: 'v1' }]
    const { db, query } = createStorageKnexQueryHarness(rows)

    await expect(
      db.listObjectsForSignatureGeneration({
        force: true,
        limit: 25,
      })
    ).resolves.toBe(rows)

    expect(query.calls).not.toContainEqual({ method: 'whereNull', args: ['signature'] })
  })

  it('matches null object versions when updating a signature without a version', async () => {
    const signature = Buffer.from('a'.repeat(64), 'hex')
    const { db, query, transaction } = createStorageKnexQueryHarness(1)

    await expect(db.updateObjectSignature('bucket-a', 'a.txt', undefined, signature)).resolves.toBe(
      true
    )

    expect(transaction.from).toHaveBeenCalledWith('objects')
    expect(query.calls).toEqual([
      { method: 'where', args: ['bucket_id', 'bucket-a'] },
      { method: 'where', args: ['name', 'a.txt'] },
      { method: 'whereNull', args: ['version'] },
      { method: 'update', args: [{ signature }] },
      { method: 'abortOnSignal', args: [undefined] },
    ])
  })

  it('matches explicit object versions when updating a signature with a version', async () => {
    const signature = Buffer.from('b'.repeat(64), 'hex')
    const { db, query, transaction } = createStorageKnexQueryHarness(1)

    await expect(db.updateObjectSignature('bucket-a', 'a.txt', 'v1', signature)).resolves.toBe(true)

    expect(transaction.from).toHaveBeenCalledWith('objects')
    expect(query.calls).toEqual([
      { method: 'where', args: ['bucket_id', 'bucket-a'] },
      { method: 'where', args: ['name', 'a.txt'] },
      { method: 'where', args: ['version', 'v1'] },
      { method: 'update', args: [{ signature }] },
      { method: 'abortOnSignal', args: [undefined] },
    ])
  })
})
