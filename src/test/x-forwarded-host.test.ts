'use strict'
import { adminApp } from './common'
import dotenv from 'dotenv'
import * as migrate from '../database/migrate'
import { knex } from '../database/multitenant-db'
import app from '../app'
import * as tenant from '../database/tenant'

dotenv.config({ path: '.env.test' })

const ENV = process.env

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
    },
  }))

  jest
    .spyOn(tenant, 'getServiceKey')
    .mockResolvedValue(Promise.resolve(process.env.SERVICE_KEY || ''))
})

beforeEach(() => {
  process.env = { ...ENV }
  process.env.IS_MULTITENANT = 'true'
  process.env.X_FORWARDED_HOST_REGEXP = '^([a-z]{20})\\.supabase\\.(?:co|in|net)$'
})

afterAll(async () => {
  await knex.destroy()
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
