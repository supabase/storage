'use strict'
import { getConfig, mergeConfig } from '../config'
import * as tenant from '../internal/database/tenant'

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
    purgeCache: {
      enabled: true,
    },
  },
}))

// Mock module with inline implementation that doesn't depend on variables
jest.mock('@storage/database', () => ({
  StorageKnexDB: jest.fn().mockImplementation(() => ({
    listBuckets: jest.fn().mockResolvedValue([{ id: 'abc123', name: 'def456' }]),
  })),
}))

// Access the mock after it's been created by the Jest runtime
const storageDbMock = require('@storage/database').StorageKnexDB

// Use this reference in tests

getConfig()
mergeConfig({
  isMultitenant: true,
  requestXForwardedHostRegExp: '^([a-z]{20})\\.supabase\\.(?:co|in|net)$',
})

import { adminApp } from './common'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import app from '../app'
import { FastifyInstance } from 'fastify'

let appInstance: FastifyInstance

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()

  jest
    .spyOn(tenant, 'getServiceKey')
    .mockResolvedValue(Promise.resolve(process.env.SERVICE_KEY || ''))
})

beforeEach(() => {
  mergeConfig({
    isMultitenant: true,
    requestXForwardedHostRegExp: '^([a-z]{20})\\.supabase\\.(?:co|in|net)$',
  })
  appInstance = app()
})

afterEach(async () => {
  await appInstance.close()
})

afterAll(async () => {
  await multitenantKnex.destroy()
  jest.restoreAllMocks()
})

describe('with X-Forwarded-Host header', () => {
  test('PostgREST URL is constructed using X-Forwarded-Host if regexp matches', async () => {
    const tenantId = 'abcdefghijklmnzzzzzz'
    const host = tenantId + '.supabase.co'
    const tenantCreateResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/` + tenantId,
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
    expect(tenantCreateResponse.statusCode).toBe(201)

    const response = await appInstance.inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-forwarded-host': host,
      },
    })
    expect(response.statusCode).toBe(200)

    await adminApp.inject({
      method: 'DELETE',
      url: '/tenants/' + tenantId,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    // check that x-forwarded-host tenant id was passed all the way to the database correctly
    expect(storageDbMock).toHaveBeenCalledTimes(1)
    expect(storageDbMock.mock.calls[0][0].options.tenantId).toBe(tenantId)
    expect(storageDbMock.mock.calls[0][0].options.host).toBe(host)
    expect(storageDbMock.mock.calls[0][1].tenantId).toBe(tenantId)
    expect(storageDbMock.mock.calls[0][1].host).toBe(host)
  })

  test('Error is thrown if X-Forwarded-Host is not present', async () => {
    const response = await appInstance.inject({
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
    const response = await appInstance.inject({
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
