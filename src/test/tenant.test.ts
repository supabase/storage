'use strict'
import { encrypt, signJWT } from '@internal/auth'
import { TENANT_CONFIG_CACHE_NAME } from '@internal/cache'
import { DBMigration } from '@internal/database/migrations'
import {
  deleteTenantConfig,
  getFeatures,
  getFileSizeLimit,
  getServiceKey,
  getTenantConfig,
} from '@internal/database/tenant'
import { cacheRequestsTotal } from '@internal/monitoring/metrics'
import dotenv from 'dotenv'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { adminApp } from './common'
import { assertLogicalLookupMetrics } from './utils/cache-metrics'
import { mockCreateLruCache } from './utils/cache-mock'

dotenv.config({ path: '.env.test' })

const serviceKeyPayload = { abc: 123 }
const testTenantIds = ['abc', 'cache-test-abc'] as const

const migrationVersion = Object.entries(DBMigration).sort(([_, a], [__, b]) => b - a)[0][0]

const payload = {
  anonKey: 'a',
  databasePoolMode: null,
  databaseUrl: 'b',
  databasePoolUrl: 'v',
  maxConnections: 12,
  fileSizeLimit: 1,
  jwtSecret: 'c',
  serviceKey: 'd',
  jwks: { keys: [] },
  migrationStatus: 'COMPLETED',
  migrationVersion,
  tracingMode: 'basic',
  capabilities: {
    list_V2: true,
    iceberg_catalog: true,
  },
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
    icebergCatalog: {
      enabled: true,
      maxCatalogs: 2,
      maxNamespaces: 10,
      maxTables: 10,
    },
    vectorBuckets: {
      enabled: true,
      maxBuckets: 2,
      maxIndexes: 10,
    },
  },
  disableEvents: null,
}

const payload2 = {
  anonKey: 'e',
  databasePoolMode: null,
  databaseUrl: 'f',
  databasePoolUrl: 'm',
  maxConnections: 14,
  fileSizeLimit: 2,
  jwtSecret: 'g',
  serviceKey: 'h',
  jwks: null,
  migrationStatus: 'COMPLETED',
  migrationVersion,
  tracingMode: 'basic',
  capabilities: {
    list_V2: true,
    iceberg_catalog: true,
  },
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
    icebergCatalog: {
      enabled: true,
      maxCatalogs: 2,
      maxNamespaces: 10,
      maxTables: 10,
    },
    vectorBuckets: {
      enabled: true,
      maxBuckets: 2,
      maxIndexes: 10,
    },
  },
  disableEvents: null,
}

type TenantModule = typeof import('../internal/database/tenant')
type MultitenantDbModule = typeof import('../internal/database/multitenant-db')

async function loadTenantModule(
  maxItems: number
): Promise<{ tenantModule: TenantModule; multitenantDbModule: MultitenantDbModule }> {
  jest.resetModules()
  mockCreateLruCache({ max: maxItems })

  return {
    tenantModule: await import('../internal/database/tenant'),
    multitenantDbModule: await import('../internal/database/multitenant-db'),
  }
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
  payload.serviceKey = await signJWT(serviceKeyPayload, payload.jwtSecret, 100)
})

async function cleanupTestTenants() {
  for (const tenantId of testTenantIds) {
    await adminApp.inject({
      method: 'DELETE',
      url: `/tenants/${tenantId}`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
  }
}

beforeEach(async () => {
  await cleanupTestTenants()
})

afterEach(async () => {
  await cleanupTestTenants()
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
    const { capabilities, ...finalPayload } = payload

    expect(responseJSON).toEqual([
      {
        id: 'abc',
        ...finalPayload,
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

    await expect(getServiceKey('abc')).resolves.toBe(payload.serviceKey)
    await expect(getFileSizeLimit('abc')).resolves.toBe(payload.fileSizeLimit)
    await expect(getFeatures('abc')).resolves.toEqual(payload.features)
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

  test('Update tenant databasePoolUrl to null', async () => {
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
      payload: { databasePoolUrl: null },
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
    expect(getResponse.statusCode).toBe(200)

    const getResponseJSON = JSON.parse(getResponse.body)
    expect(getResponseJSON).toEqual({ ...payload, databasePoolUrl: null })
    expect(getResponseJSON.databasePoolUrl).toBeNull()
  })

  test('Upsert tenant config updates iceberg/vector limits when enabled is omitted', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const updatedValue = 999

    const putPayloadWithoutFeatureEnabled = {
      ...payload,
      features: {
        ...payload.features,
        icebergCatalog: {
          maxCatalogs: updatedValue,
          maxNamespaces: updatedValue,
          maxTables: updatedValue,
        },
        vectorBuckets: {
          maxBuckets: updatedValue,
          maxIndexes: updatedValue,
        },
      },
    }

    const putResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: putPayloadWithoutFeatureEnabled,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(putResponse.statusCode).toBe(204)

    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse.statusCode).toBe(200)

    const getResponseJSON = JSON.parse(getResponse.body)
    expect(getResponseJSON.features.icebergCatalog).toEqual({
      enabled: payload.features.icebergCatalog.enabled,
      maxCatalogs: updatedValue,
      maxNamespaces: updatedValue,
      maxTables: updatedValue,
    })
    expect(getResponseJSON.features.vectorBuckets).toEqual({
      enabled: payload.features.vectorBuckets.enabled,
      maxBuckets: updatedValue,
      maxIndexes: updatedValue,
    })
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

  test('Get tenant config with invalid tenant id expected error', async () => {
    await expect(getTenantConfig('')).rejects.toThrow('Invalid tenant id')
  })

  test('Get tenant config with unknown tenant id expected error', async () => {
    await expect(getTenantConfig('zzz')).rejects.toThrow('Missing tenant config for tenant zzz')
  })

  test('Get tenant config always retrieves concurrent requests from cache', async () => {
    const tenantId = 'cache-test-abc'
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    try {
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

  test('Get tenant config evicts cold tenants from cache', async () => {
    const tenantIds = ['cache-eviction-1', 'cache-eviction-2', 'cache-eviction-3']
    const encryptedTenant = {
      anon_key: encrypt('anon'),
      database_url: encrypt('postgres://tenant'),
      database_pool_mode: null,
      file_size_limit: 1,
      jwt_secret: encrypt('jwt-secret'),
      jwks: null,
      service_key: encrypt('service-key'),
      feature_purge_cache: false,
      feature_image_transformation: false,
      feature_s3_protocol: false,
      feature_iceberg_catalog: false,
      feature_iceberg_catalog_max_catalogs: 0,
      feature_iceberg_catalog_max_namespaces: 0,
      feature_iceberg_catalog_max_tables: 0,
      feature_vector_buckets: false,
      feature_vector_buckets_max_buckets: 0,
      feature_vector_buckets_max_indexes: 0,
      image_transformation_max_resolution: null,
      database_pool_url: null,
      max_connections: null,
      migrations_version: migrationVersion,
      migrations_status: 'COMPLETED',
      tracing_mode: null,
      disable_events: null,
    }

    const { tenantModule, multitenantDbModule } = await loadTenantModule(2)
    const knexTableSpy = jest.spyOn(multitenantDbModule.multitenantKnex, 'table')
    const queryBuilder = {
      first: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      abortOnSignal: jest.fn().mockResolvedValue(encryptedTenant),
    }

    try {
      knexTableSpy.mockReturnValue(
        queryBuilder as unknown as ReturnType<typeof multitenantKnex.table>
      )

      for (const tenantId of tenantIds) {
        await tenantModule.getTenantConfig(tenantId)
      }

      expect(knexTableSpy).toHaveBeenCalledTimes(tenantIds.length)

      await tenantModule.getTenantConfig(tenantIds[0])

      expect(knexTableSpy).toHaveBeenCalledTimes(tenantIds.length + 1)
    } finally {
      tenantIds.forEach((tenantId) => {
        tenantModule.deleteTenantConfig(tenantId)
      })
      jest.dontMock('@internal/cache')
      jest.resetModules()
      knexTableSpy.mockRestore()
    }
  })

  test('Get tenant config records one cache request per logical lookup', async () => {
    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const tenantId = 'cache-metrics-lookup'
    const encryptedTenant = {
      anon_key: encrypt('anon'),
      database_url: encrypt('postgres://tenant'),
      database_pool_mode: null,
      file_size_limit: 1,
      jwt_secret: encrypt('jwt-secret'),
      jwks: null,
      service_key: encrypt('service-key'),
      feature_purge_cache: false,
      feature_image_transformation: false,
      feature_s3_protocol: false,
      feature_iceberg_catalog: false,
      feature_iceberg_catalog_max_catalogs: 0,
      feature_iceberg_catalog_max_namespaces: 0,
      feature_iceberg_catalog_max_tables: 0,
      feature_vector_buckets: false,
      feature_vector_buckets_max_buckets: 0,
      feature_vector_buckets_max_indexes: 0,
      image_transformation_max_resolution: null,
      database_pool_url: null,
      max_connections: null,
      migrations_version: migrationVersion,
      migrations_status: 'COMPLETED',
      tracing_mode: null,
      disable_events: null,
    }

    const tenantQuery = Promise.withResolvers<typeof encryptedTenant>()
    const queryBuilder = {
      first: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      abortOnSignal: jest.fn().mockImplementation(() => tenantQuery.promise),
    }

    try {
      knexTableSpy.mockReturnValue(
        queryBuilder as unknown as ReturnType<typeof multitenantKnex.table>
      )
      await assertLogicalLookupMetrics({
        addSpy,
        backendCallSpy: queryBuilder.abortOnSignal,
        cacheName: TENANT_CONFIG_CACHE_NAME,
        startLookups: () => [
          getTenantConfig(tenantId),
          getTenantConfig(tenantId),
          getTenantConfig(tenantId),
        ],
        resolveBackend: () => tenantQuery.resolve(encryptedTenant),
        assertCachedHit: async () => {
          await expect(getTenantConfig(tenantId)).resolves.toMatchObject({
            databaseUrl: 'postgres://tenant',
          })
        },
      })
    } finally {
      deleteTenantConfig(tenantId)
      knexTableSpy.mockRestore()
      addSpy.mockRestore()
    }
  })
})
