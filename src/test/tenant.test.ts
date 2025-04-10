'use strict'
import dotenv from 'dotenv'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { adminApp } from './common'
import {
  getFeatures,
  getFileSizeLimit,
  getServiceKey,
  getTenantConfig,
} from '@internal/database/tenant'
import { signJWT } from '@internal/auth'

dotenv.config({ path: '.env.test' })

const serviceKeyPayload = { abc: 123 }

const payload = {
  anonKey: 'a',
  databaseUrl: 'b',
  databasePoolUrl: 'v',
  maxConnections: 12,
  fileSizeLimit: 1,
  jwtSecret: 'c',
  serviceKey: 'd',
  migrationStatus: 'COMPLETED',
  migrationVersion: 'optimise-existing-functions',
  tracingMode: 'basic',
  features: {
    imageTransformation: {
      enabled: true,
      maxResolution: null,
    },
    s3Protocol: {
      enabled: true,
    },
    purgeCache: {
      enabled: false,
    },
  },
  disableEvents: null,
}

const payload2 = {
  anonKey: 'e',
  databaseUrl: 'f',
  databasePoolUrl: 'm',
  maxConnections: 14,
  fileSizeLimit: 2,
  jwtSecret: 'g',
  serviceKey: 'h',
  migrationStatus: 'COMPLETED',
  migrationVersion: 'optimise-existing-functions',
  tracingMode: 'basic',
  features: {
    imageTransformation: {
      enabled: false,
      maxResolution: null,
    },
    s3Protocol: {
      enabled: true,
    },
    purgeCache: {
      enabled: true,
    },
  },
  disableEvents: null,
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
  payload.serviceKey = await signJWT(serviceKeyPayload, payload.jwtSecret, 100)
})

afterEach(async () => {
  await adminApp.inject({
    method: 'DELETE',
    url: '/tenants/abc',
    headers: {
      apikey: process.env.ADMIN_API_KEYS,
    },
  })
})

afterAll(async () => {
  await multitenantKnex.destroy()
})

describe('Tenant configs', () => {
  test('Get all tenant configs', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const response = await adminApp.inject({
      method: 'GET',
      url: `/tenants`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toEqual([
      {
        id: 'abc',
        ...payload,
      },
    ])
  })

  test('Get nonexistent tenant config', async () => {
    const response = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(404)
  })

  test('Get existing tenant config', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const response = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toEqual(payload)

    expect(getServiceKey('abc')).resolves.toBe(payload.serviceKey)
    expect(getFileSizeLimit('abc')).resolves.toBe(payload.fileSizeLimit)
    expect(getFeatures('abc')).resolves.toEqual(payload.features)
  })

  test('Insert tenant config without required properties', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload: {},
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('Insert tenant config twice', async () => {
    const firstInsertResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(firstInsertResponse.statusCode).toBe(201)
    const secondInsertResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(secondInsertResponse.statusCode).toBe(500)
  })

  test('Update tenant config', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const patchResponse = await adminApp.inject({
      method: 'PATCH',
      url: `/tenants/abc`,
      payload: payload2,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(patchResponse.statusCode).toBe(204)
    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const getResponseJSON = JSON.parse(getResponse.body)
    expect(getResponseJSON).toEqual(payload2)
  })

  test('Update tenant config partially', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const patchResponse = await adminApp.inject({
      method: 'PATCH',
      url: `/tenants/abc`,
      payload: { fileSizeLimit: 2 },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(patchResponse.statusCode).toBe(204)
    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const getResponseJSON = JSON.parse(getResponse.body)
    expect(getResponseJSON).toEqual({ ...payload, fileSizeLimit: 2 })
  })

  test('Upsert tenant config', async () => {
    const firstPutResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(firstPutResponse.statusCode).toBe(204)
    const firstGetResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const firstGetResponseJSON = JSON.parse(firstGetResponse.body)
    expect(firstGetResponseJSON).toEqual(payload)
    const secondPutResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: payload2,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(secondPutResponse.statusCode).toBe(204)
    const secondGetResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const secondGetResponseJSON = JSON.parse(secondGetResponse.body)
    expect(secondGetResponseJSON).toEqual(payload2)
  })

  test('Delete tenant config', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    const deleteResponse = await adminApp.inject({
      method: 'DELETE',
      url: '/tenants/abc',
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(deleteResponse.statusCode).toBe(204)
    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse.statusCode).toBe(404)
  })

  test('Get tenant config with invalid tenant id expected error', () => {
    expect(getTenantConfig('')).rejects.toThrowError('Invalid tenant id')
  })

  test('Get tenant config with unknown tenant id expected error', () => {
    expect(getTenantConfig('zzz')).rejects.toThrowError('Missing tenant config for tenant zzz')
  })

  test('Get tenant config always retrieves concurrent requests from cache', async () => {
    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    try {
      const tenantId = 'cache-test-abc'
      await adminApp.inject({
        method: 'POST',
        url: `/tenants/${tenantId}`,
        payload,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      await getTenantConfig(tenantId)
      expect(knexTableSpy).toHaveBeenCalledTimes(1)
      expect(knexTableSpy).toHaveBeenCalledWith('tenants')

      const results = await Promise.all([
        getTenantConfig(tenantId),
        getTenantConfig(tenantId),
        getTenantConfig(tenantId),
      ])
      expect(knexTableSpy).toHaveBeenCalledTimes(1)
      results.forEach((result, i) => expect(result).toEqual(results[i === 0 ? 1 : 0]))

      await adminApp.inject({
        method: 'DELETE',
        url: `/tenants/${tenantId}`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
    } finally {
      knexTableSpy.mockRestore()
    }
  })
})
