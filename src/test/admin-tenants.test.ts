import { closeMultitenantPg } from '@internal/database'
import * as migrations from '@internal/database/migrations'
import { adminApp } from './common'

const adminHeaders = {
  apikey: process.env.ADMIN_API_KEYS!,
  'content-type': 'application/json',
}

const baseTenantBody = {
  anonKey: 'anon',
  databaseUrl: 'postgresql://postgres:postgres@127.0.0.1:5433/postgres',
  jwtSecret: 'secret',
  serviceKey: 'service',
  jwks: null,
  fileSizeLimit: 1024,
}

beforeAll(async () => {
  await migrations.runMultitenantMigrations()
})

afterAll(async () => {
  await adminApp.close()
  await multitenantKnex.destroy()
})

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
      headers: adminHeaders,
      payload: '',
    })

    expect(response.statusCode).toBe(204)
    expect(response.body).toBe('')
  })
})

describe('admin tenant put route nullable pool fields', () => {
  const tenantId = 'put-nullable-pool'

  beforeAll(async () => {
    await multitenantKnex('tenants').where('id', tenantId).delete()
  })

  afterAll(async () => {
    await multitenantKnex('tenants').where('id', tenantId).delete()
  })

  it('clears database_pool_url and database_pool_mode when put with null', async () => {
    const initial = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}`,
      headers: adminHeaders,
      payload: JSON.stringify({
        ...baseTenantBody,
        databasePoolUrl: 'postgresql://postgres:postgres@127.0.0.1:6454/postgres',
        databasePoolMode: 'transaction',
      }),
    })
    expect(initial.statusCode).toBe(204)

    const seeded = await multitenantKnex('tenants').first().where('id', tenantId)
    expect(seeded?.database_pool_url).toBeTruthy()
    expect(seeded?.database_pool_mode).toBe('transaction')

    const cleared = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}`,
      headers: adminHeaders,
      payload: JSON.stringify({
        ...baseTenantBody,
        databasePoolUrl: null,
        databasePoolMode: null,
      }),
    })
    expect(cleared.statusCode).toBe(204)

    const after = await multitenantKnex('tenants').first().where('id', tenantId)
    expect(after?.database_pool_url).toBeNull()
    expect(after?.database_pool_mode).toBeNull()
  })
})
