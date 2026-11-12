import { closeMultitenantPg, multitenantPgExecutor } from '@internal/database'
import * as migrations from '@internal/database/migrations'
import { DeleteIcebergResources } from '@storage/events'
import { adminApp } from './common'

describe('admin tenant delete route', () => {
  beforeAll(async () => {
    await migrations.runMultitenantMigrations()
  })

  afterAll(async () => {
    await adminApp.close()
    await closeMultitenantPg()
  })

  it('accepts an empty json delete request', async () => {
    const response = await adminApp.inject({
      method: 'DELETE',
      url: '/tenants/abc',
      headers: {
        apikey: process.env.ADMIN_API_KEYS!,
        'content-type': 'application/json',
      },
      payload: '',
    })

    expect(response.statusCode).toBe(204)
    expect(response.body).toBe('')
  })

  describe('purge', () => {
    const tenantId = 'purge-tenant'
    const otherTenantId = 'purge-other-tenant'

    async function insertCatalog(tenant: string, name: string) {
      const { rows } = await multitenantPgExecutor.query<{ id: string }>({
        text: 'INSERT INTO iceberg_catalogs (name, tenant_id) VALUES ($1, $2) RETURNING id',
        values: [name, tenant],
      })
      return rows[0].id
    }

    async function deletedAt(id: string) {
      const { rows } = await multitenantPgExecutor.query<{ deleted_at: Date | null }>({
        text: 'SELECT deleted_at FROM iceberg_catalogs WHERE id = $1',
        values: [id],
      })
      return rows[0].deleted_at
    }

    function deleteTenant(url: string) {
      return adminApp.inject({
        method: 'DELETE',
        url,
        headers: { apikey: process.env.ADMIN_API_KEYS! },
      })
    }

    afterEach(async () => {
      await multitenantPgExecutor.query({
        text: 'DELETE FROM iceberg_catalogs WHERE tenant_id = ANY($1)',
        values: [[tenantId, otherTenantId]],
      })
    })

    it('leaves Iceberg catalogs untouched without purge', async () => {
      const catalogId = await insertCatalog(tenantId, 'analytics')

      const batchSend = vi.spyOn(DeleteIcebergResources, 'batchSend').mockResolvedValue(undefined)

      const response = await deleteTenant(`/tenants/${tenantId}`)

      expect(response.statusCode).toBe(204)
      expect(await deletedAt(catalogId)).toBeNull()
      expect(batchSend).not.toHaveBeenCalled()
    })

    it("soft-deletes only the tenant's Iceberg catalogs with purge=true", async () => {
      const catalogId = await insertCatalog(tenantId, 'analytics')
      const otherCatalogId = await insertCatalog(otherTenantId, 'analytics')

      const batchSend = vi.spyOn(DeleteIcebergResources, 'batchSend').mockResolvedValue(undefined)

      const response = await deleteTenant(`/tenants/${tenantId}?purge=true`)

      expect(response.statusCode).toBe(204)
      expect(await deletedAt(catalogId)).toBeInstanceOf(Date)
      expect(await deletedAt(otherCatalogId)).toBeNull()

      expect(batchSend).toHaveBeenCalledTimes(1)
      expect(batchSend.mock.calls[0][0]).toStrictEqual([
        { tenant: { ref: tenantId, host: '' }, catalogId, sbReqId: undefined },
      ])
    })
  })
})
