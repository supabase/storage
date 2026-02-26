import { getServiceKeyUser } from '@internal/database'
import { TenantConnection } from '@internal/database/connection'
import { PoolManager } from '@internal/database/pool'
import { getConfig } from '../config'

const { databaseURL, databasePoolURL, tenantId } = getConfig()

describe('Query Abort Signal', () => {
  let poolManager: PoolManager
  let pool: ReturnType<PoolManager['getPool']>

  beforeAll(async () => {
    const superUser = await getServiceKeyUser(tenantId)
    poolManager = new PoolManager()
    pool = poolManager.getPool({
      tenantId,
      isExternalPool: true,
      maxConnections: 5,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })
  })

  afterAll(async () => {
    await pool.destroy()
  })

  describe('abortOnSignal on Raw queries', () => {
    it('should execute query normally when signal is not aborted', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      const result = await conn.raw('SELECT 1 as test').abortOnSignal(controller.signal)

      expect(result.rows[0].test).toEqual(1)
    })

    it('should abort query when signal is aborted before execution', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      // Abort before creating the query
      controller.abort()

      await expect(async () => {
        conn.raw('SELECT 1 as test').abortOnSignal(controller.signal)
      }).rejects.toThrow('Signal is already aborted')
    })

    it('should abort a long-running query when signal is aborted', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      // Start a slow query (pg_sleep for 10 seconds)
      const queryPromise = conn.raw('SELECT pg_sleep(10)').abortOnSignal(controller.signal)

      // Abort after a short delay
      setTimeout(() => controller.abort(), 100)

      await expect(queryPromise).rejects.toMatchObject({
        name: 'AbortError',
        code: 'ABORT_ERR',
        message: 'Query was aborted',
      })
    })

    it('should reject with AbortError containing correct properties', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      const queryPromise = conn.raw('SELECT pg_sleep(5)').abortOnSignal(controller.signal)

      setTimeout(() => controller.abort(), 50)

      try {
        await queryPromise
        fail('Expected query to be aborted')
      } catch (error: any) {
        expect(error.name).toBe('AbortError')
        expect(error.code).toBe('ABORT_ERR')
        expect(error.message).toBe('Query was aborted')
      }
    })

    it('should throw error for invalid signal parameter', async () => {
      const conn = pool.acquire()

      expect(() => {
        conn.raw('SELECT 1').abortOnSignal('not a signal' as any)
      }).toThrow('Expected signal to be an instance of AbortSignal')
    })
  })

  describe('abortOnSignal on Query Builder', () => {
    it('should execute query normally when signal is not aborted', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      const result = await conn.select(conn.raw('1 as test')).abortOnSignal(controller.signal)

      expect(result[0].test).toEqual(1)
    })

    it('should abort query builder query when signal is aborted', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      const queryPromise = conn.select(conn.raw('pg_sleep(10)')).abortOnSignal(controller.signal)

      setTimeout(() => controller.abort(), 100)

      await expect(queryPromise).rejects.toMatchObject({
        name: 'AbortError',
      })
    })
  })

  describe('abortOnSignal with timeout', () => {
    it('should work with both timeout and abortSignal', async () => {
      const controller = new AbortController()
      const conn = pool.acquire()

      const queryPromise = conn
        .raw('SELECT pg_sleep(10)')
        .timeout(5000, { cancel: true })
        .abortOnSignal(controller.signal)

      // Abort should win over timeout since we abort immediately
      setTimeout(() => controller.abort(), 50)

      await expect(queryPromise).rejects.toMatchObject({
        name: 'AbortError',
      })
    })
  })
})

describe('Statement Timeout', () => {
  it('should apply SET LOCAL statement_timeout in transactions', async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const poolManager = new PoolManager()
    const pool = poolManager.getPool({
      tenantId,
      isExternalPool: true,
      maxConnections: 2,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const connection = new TenantConnection(pool, {
      tenantId,
      isExternalPool: true,
      maxConnections: 2,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const tnx = await connection.transaction()

    try {
      // Check that statement_timeout is set
      const result = await tnx.raw('SHOW statement_timeout')
      const timeout = result.rows[0].statement_timeout

      // Should be set to 30s (default) or whatever DATABASE_STATEMENT_TIMEOUT is set to
      expect(timeout).toBeTruthy()
      expect(timeout).not.toBe('0')

      await tnx.commit()
    } catch (e) {
      await tnx.rollback()
      throw e
    } finally {
      await pool.destroy()
    }
  })
})

describe('TenantConnection Abort Signal', () => {
  it('should store and retrieve abort signal', async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const poolManager = new PoolManager()
    const pool = poolManager.getPool({
      tenantId,
      isExternalPool: true,
      maxConnections: 2,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const connection = new TenantConnection(pool, {
      tenantId,
      isExternalPool: true,
      maxConnections: 2,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    // Initially no signal
    expect(connection.getAbortSignal()).toBeUndefined()

    // Set signal
    const controller = new AbortController()
    connection.setAbortSignal(controller.signal)

    // Should retrieve the same signal
    expect(connection.getAbortSignal()).toBe(controller.signal)

    await pool.destroy()
  })
})
