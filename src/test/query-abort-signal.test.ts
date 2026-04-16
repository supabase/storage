import { getServiceKeyUser } from '@internal/database'
import { TenantConnection } from '@internal/database/connection'
import { PoolManager } from '@internal/database/pool'
import { getConfig } from '../config'

const { databaseURL, databasePoolURL, tenantId } = getConfig()
type TestPool = ReturnType<PoolManager['getPool']>
type TestConnection = ReturnType<TestPool['acquire']>

describe('Query Abort Signal', () => {
  let poolManager: PoolManager
  let superUser: Awaited<ReturnType<typeof getServiceKeyUser>>

  beforeAll(async () => {
    superUser = await getServiceKeyUser(tenantId)
    poolManager = new PoolManager()
  })

  async function withIsolatedConnection<T>(run: (conn: TestConnection) => Promise<T>) {
    // Force fresh because single-use external requests still reuse
    // an already cached pool for the same tenant.
    await poolManager.destroy(tenantId)

    // Use an uncached single-use pool so aborted queries can't
    // leak disposed connections into the next test case while Knex
    // finishes tearing them down in the background.
    const pool = poolManager.getPool({
      tenantId,
      isSingleUse: true,
      isExternalPool: true,
      maxConnections: 1,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const conn = pool.acquire()

    try {
      await conn.raw('SELECT 1')
      return await run(conn)
    } finally {
      try {
        await pool.destroy()
      } finally {
        await poolManager.destroy(tenantId)
      }
    }
  }

  describe('abortOnSignal on Raw queries', () => {
    it('should execute query normally when signal is not aborted', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        const result = await conn.raw('SELECT 1 as test').abortOnSignal(controller.signal)

        expect(result.rows[0].test).toEqual(1)
      })
    })

    it('should abort query when signal is aborted before execution', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        // Abort before creating the query
        controller.abort()

        await expect(
          Promise.resolve().then(() =>
            conn.raw('SELECT 1 as test').abortOnSignal(controller.signal)
          )
        ).rejects.toThrow('Signal is already aborted')
      })
    })

    it('should abort a long-running query when signal is aborted', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        // Start a slow query (pg_sleep for 10 seconds)
        const queryPromise = conn.raw('SELECT pg_sleep(10)').abortOnSignal(controller.signal)

        // Abort after a short delay
        const abortTimeout = setTimeout(() => controller.abort(), 100)

        try {
          await expect(queryPromise).rejects.toMatchObject({
            name: 'AbortError',
            code: 'ABORT_ERR',
            message: 'Query was aborted',
          })
        } finally {
          clearTimeout(abortTimeout)
        }
      })
    })

    it('should reject with AbortError containing correct properties', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        const queryPromise = conn.raw('SELECT pg_sleep(5)').abortOnSignal(controller.signal)

        const abortTimeout = setTimeout(() => controller.abort(), 50)

        try {
          await queryPromise
          throw new Error('Expected query to be aborted')
        } catch (error: unknown) {
          expect(error).toMatchObject({
            name: 'AbortError',
            code: 'ABORT_ERR',
            message: 'Query was aborted',
          })
        } finally {
          clearTimeout(abortTimeout)
        }
      })
    })

    it('should throw error for invalid signal parameter', async () => {
      await withIsolatedConnection(async (conn) => {
        const invalidSignal = 'not a signal' as unknown as AbortSignal

        expect(() => {
          conn.raw('SELECT 1').abortOnSignal(invalidSignal)
        }).toThrow('Expected signal to be an instance of AbortSignal')
      })
    })
  })

  describe('abortOnSignal on Query Builder', () => {
    it('should execute query normally when signal is not aborted', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        const result = await conn.select(conn.raw('1 as test')).abortOnSignal(controller.signal)

        expect(result[0].test).toEqual(1)
      })
    })

    it('should abort query builder query when signal is aborted', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        const queryPromise = conn.select(conn.raw('pg_sleep(10)')).abortOnSignal(controller.signal)

        const abortTimeout = setTimeout(() => controller.abort(), 100)

        try {
          await expect(queryPromise).rejects.toMatchObject({
            name: 'AbortError',
          })
        } finally {
          clearTimeout(abortTimeout)
        }
      })
    })
  })

  describe('abortOnSignal with timeout', () => {
    it('should work with both timeout and abortSignal', async () => {
      await withIsolatedConnection(async (conn) => {
        const controller = new AbortController()

        const queryPromise = conn
          .raw('SELECT pg_sleep(10)')
          .timeout(5000, { cancel: true })
          .abortOnSignal(controller.signal)

        // Abort should win over timeout since we abort immediately
        const abortTimeout = setTimeout(() => controller.abort(), 50)

        try {
          await expect(queryPromise).rejects.toMatchObject({
            name: 'AbortError',
          })
        } finally {
          clearTimeout(abortTimeout)
        }
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
