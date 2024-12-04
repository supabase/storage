'use strict'
import { adminApp } from './common'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import app from '../app'
import * as tenant from '../internal/database/tenant'
import { getConfig, mergeConfig } from '../config'

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
  jest.spyOn(tenant, 'getTenantConfig').mockImplementation(async () => ({
    anonKey: process.env.ANON_KEY || '',
    databaseUrl: process.env.DATABASE_URL || '',
    serviceKey: process.env.SERVICE_KEY || '',
    serviceKeyPayload: {
      alg: 'HS256',
      typ: 'JWT',
      role: 'service_role',
      iat: 1613531985,
      exp: 1929107985,
    },
    jwtSecret: process.env.PGRST_JWT_SECRET || '',
    fileSizeLimit: parseInt(process.env.FILE_SIZE_LIMIT || '1000'),
    features: {
      imageTransformation: {
        enabled: true,
      },
      s3Protocol: {
        enabled: true,
      },
    },
  }))

  jest
    .spyOn(tenant, 'getServiceKey')
    .mockResolvedValue(Promise.resolve(process.env.SERVICE_KEY || ''))
})

beforeEach(() => {
  mergeConfig({
    isMultitenant: true,
    requestXForwardedHostRegExp: '^([a-z]{20})\\.supabase\\.(?:co|in|net)$',
  })
})

afterEach(() => {
  getConfig({ reload: true })
})

afterAll(async () => {
  await multitenantKnex.destroy()
})

describe('with X-Forwarded-Host header', () => {
  test('PostgREST URL is constructed using X-Forwarded-Host if regexp matches', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abcdefghijklmnopqrst`,
      payload: {
        anonKey: 'a',
        databaseUrl: 'b',
        jwtSecret: 'c',
        serviceKey: 'd',
      },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-forwarded-host': 'abcdefghijklmnopqrst.supabase.co',
      },
    })
    expect(response.statusCode).toBe(200)
    await adminApp.inject({
      method: 'DELETE',
      url: '/tenants/abcdefghijklmnopqrst',
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
  })

  test('Error is thrown if X-Forwarded-Host is not present', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toBe('X-Forwarded-Host header is not a string')
  })

  test('Error is thrown if X-Forwarded-Host does not match regexp', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-forwarded-host': 'abcdefghijklmnopqrst.supabase.com',
      },
    })
    expect(response.statusCode).toBe(400)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toBe('X-Forwarded-Host header does not match regular expression')
  })
})
