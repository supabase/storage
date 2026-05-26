import * as migrations from '@internal/database/migrations'
import { multitenantKnex } from '@internal/database/multitenant-db'
import { adminApp } from './common'

describe('admin tenant delete route', () => {
  beforeAll(async () => {
    await migrations.runMultitenantMigrations()
  })

  afterAll(async () => {
    await adminApp.close()
    await multitenantKnex.destroy()
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
})
