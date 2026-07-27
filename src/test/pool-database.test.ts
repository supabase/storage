import { getServiceKeyUser, PgPoolManager } from '@internal/database'
import { getConfig } from '../config'

const { databaseEngine, databaseURL, databasePoolURL, tenantId } = getConfig()

function databaseConnectionString(connectionString: string, database: string): string {
  const url = new URL(connectionString)
  url.pathname = `/${database}`
  return url.toString()
}

describe('TenantPool Database', () => {
  it('can acquire a on a destroyed pool', async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const poolManager = new PgPoolManager()
    const pool = poolManager.getPool({
      tenantId,
      isExternalPool: true,
      maxConnections: 20,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const conn = pool.acquire()

    const r = await conn.query<{ n: number }>('SELECT 1 as n')
    expect(r.rows[0].n).toEqual(1)

    await pool.closeCurrentPool()

    const conn2 = pool.acquire()
    const r2 = await conn2.query<{ n: number }>('SELECT 2 as n')
    expect(r2.rows[0].n).toEqual(2)

    await pool.closeCurrentPool()
  })

  it.skipIf(databaseEngine === 'multigres')(
    'routes the next query to the reconciled database endpoint',
    async () => {
      const superUser = await getServiceKeyUser(tenantId)
      const poolManager = new PgPoolManager()
      const strategyTenantId = tenantId
      const configuredDatabase = decodeURIComponent(new URL(databaseURL).pathname.slice(1))
      const databaseA = configuredDatabase || 'postgres'
      const databaseB = databaseA === 'template1' ? 'postgres' : 'template1'
      const settings = {
        tenantId: strategyTenantId,
        isExternalPool: false,
        maxConnections: 2,
        dbUrl: databaseConnectionString(databaseURL, databaseA),
        configRevision: 1,
        user: superUser,
        superUser,
      }

      try {
        const strategy = poolManager.getPool(settings)
        const resultA = await strategy
          .acquire()
          .query<{ database: string }>('SELECT current_database() AS database')

        expect(resultA.rows[0].database).toBe(databaseA)

        const reconciled = poolManager.getPool({
          ...settings,
          dbUrl: databaseConnectionString(databaseURL, databaseB),
          configRevision: 2,
        })
        const resultB = await reconciled
          .acquire()
          .query<{ database: string }>('SELECT current_database() AS database')

        expect(reconciled).toBe(strategy)
        expect(resultB.rows[0].database).toBe(databaseB)
      } finally {
        await poolManager.retire(strategyTenantId, new Error('test cleanup'))
      }
    }
  )
})
