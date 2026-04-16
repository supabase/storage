import * as migrations from '@internal/database/migrations'
import { multitenantKnex } from '@internal/database/multitenant-db'
import { adminApp } from './common'

describe('admin orphan-objects routes', () => {
  beforeAll(async () => {
    await migrations.runMultitenantMigrations()
  })

  afterAll(async () => {
    await adminApp.close()
    await multitenantKnex.destroy()
  })

  describe('GET /tenants/:tenantId/buckets/:bucketId/orphan-objects', () => {
    it('returns 400 when the before query parameter is not a valid date', async () => {
      const response = await adminApp.inject({
        method: 'GET',
        url: '/tenants/bjhaohmqunupljrqypxz/buckets/bucket2/orphan-objects?before=not-a-date',
        headers: {
          apikey: process.env.ADMIN_API_KEYS!,
        },
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).error).toBe('Invalid date format')
    })
  })

  describe('DELETE /tenants/:tenantId/buckets/:bucketId/orphan-objects', () => {
    it('returns 400 when the before body field is not a valid date', async () => {
      const response = await adminApp.inject({
        method: 'DELETE',
        url: '/tenants/bjhaohmqunupljrqypxz/buckets/bucket2/orphan-objects',
        headers: {
          apikey: process.env.ADMIN_API_KEYS!,
          'content-type': 'application/json',
        },
        payload: JSON.stringify({
          deleteDbKeys: true,
          deleteS3Keys: false,
          before: 'not-a-date',
        }),
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).error).toBe('Invalid date format')
    })

    it('returns 400 when neither deleteDbKeys nor deleteS3Keys is set (unchanged behavior)', async () => {
      const response = await adminApp.inject({
        method: 'DELETE',
        url: '/tenants/bjhaohmqunupljrqypxz/buckets/bucket2/orphan-objects',
        headers: {
          apikey: process.env.ADMIN_API_KEYS!,
          'content-type': 'application/json',
        },
        payload: JSON.stringify({
          deleteDbKeys: false,
          deleteS3Keys: false,
        }),
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).error).toContain(
        'At least one of deleteDbKeys or deleteS3Keys'
      )
    })
  })
})
