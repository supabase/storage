import { getServiceKeyUser } from '@internal/database'
import { PoolManager } from '@internal/database/pool'
import { getConfig } from '../config'

const { databaseURL, databasePoolURL, tenantId } = getConfig()

describe('TenantPool Database', () => {
  it('can acquire a on a destroyed pool', async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const poolManager = new PoolManager()
    const pool = poolManager.getPool({
      tenantId,
      isExternalPool: true,
      maxConnections: 20,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
    })

    const conn = pool.acquire()

    const r = await conn.raw('SELECT 1 as n')
    expect(r.rows[0].n).toEqual(1)

    await pool.destroy()

    const conn2 = pool.acquire()
    const r2 = await conn2.raw('SELECT 2 as n')
    expect(r2.rows[0].n).toEqual(2)

    await pool.destroy()
  })
})
