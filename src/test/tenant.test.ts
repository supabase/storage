import { encrypt, signJWT } from '@internal/auth'
import { TENANT_CONFIG_CACHE_NAME } from '@internal/cache'
import { jwksManager } from '@internal/database'
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
type TenantQueryBuilder = ReturnType<(typeof multitenantKnex)['table']>

async function loadTenantModule(
  maxItems: number
): Promise<{ tenantModule: TenantModule; multitenantDbModule: MultitenantDbModule }> {
  vi.resetModules()
  mockCreateLruCache({ max: maxItems })

  return {
    tenantModule: await import('../internal/database/tenant'),
    multitenantDbModule: await import('../internal/database/multitenant-db'),
  }
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  vi.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
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
  await adminApp.close()
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

  test('Get tenant config omits sensitive data when ADMIN_RETURN_TENANT_SENSITIVE_DATA is false', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const previousValue = process.env.ADMIN_RETURN_TENANT_SENSITIVE_DATA
    process.env.ADMIN_RETURN_TENANT_SENSITIVE_DATA = 'false'

    try {
      let isolatedApp: ReturnType<typeof import('../admin-app').default> | undefined
      await jest.isolateModulesAsync(async () => {
        const { default: createApp } = await import('../admin-app')
        isolatedApp = createApp({})
      })

      const singleResponse = await isolatedApp!.inject({
        method: 'GET',
        url: `/tenants/abc`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(singleResponse.statusCode).toBe(200)
      const singleJSON = JSON.parse(singleResponse.body)

      expect(singleJSON.anonKey).toBeUndefined()
      expect(singleJSON.databaseUrl).toBeUndefined()
      expect(singleJSON.databasePoolUrl).toBeUndefined()
      expect(singleJSON.jwtSecret).toBeUndefined()
      expect(singleJSON.jwks).toBeUndefined()
      expect(singleJSON.serviceKey).toBeUndefined()

      // Non-sensitive fields are still returned
      expect(singleJSON.fileSizeLimit).toBe(payload.fileSizeLimit)
      expect(singleJSON.maxConnections).toBe(payload.maxConnections)
      expect(singleJSON.features).toEqual(payload.features)
      expect(singleJSON.tracingMode).toBe(payload.tracingMode)

      const listResponse = await isolatedApp!.inject({
        method: 'GET',
        url: `/tenants`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(listResponse.statusCode).toBe(200)
      const listJSON = JSON.parse(listResponse.body)
      expect(listJSON).toHaveLength(1)
      expect(listJSON[0].id).toBe('abc')
      expect(listJSON[0].anonKey).toBeUndefined()
      expect(listJSON[0].databaseUrl).toBeUndefined()
      expect(listJSON[0].databasePoolUrl).toBeUndefined()
      expect(listJSON[0].jwtSecret).toBeUndefined()
      expect(listJSON[0].jwks).toBeUndefined()
      expect(listJSON[0].serviceKey).toBeUndefined()
      expect(listJSON[0].fileSizeLimit).toBe(payload.fileSizeLimit)

      await isolatedApp!.close()
    } finally {
      if (previousValue === undefined) {
        delete process.env.ADMIN_RETURN_TENANT_SENSITIVE_DATA
      } else {
        process.env.ADMIN_RETURN_TENANT_SENSITIVE_DATA = previousValue
      }
    }
  })

  test('Create tenant config preserves disableEvents and image transformation maxResolution', async () => {
    const createPayload = {
      ...payload,
      disableEvents: ['ObjectCreated:*', 'ObjectRemoved:*'],
      features: {
        ...payload.features,
        imageTransformation: {
          ...payload.features.imageTransformation,
          maxResolution: 1024,
        },
      },
    }

    const createResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload: createPayload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(createResponse.statusCode).toBe(201)

    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse.statusCode).toBe(200)
    expect(JSON.parse(getResponse.body)).toEqual(createPayload)
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

  test('Create tenant config rolls back when jwk generation fails', async () => {
    const generateUrlSigningJwkSpy = vi
      .spyOn(jwksManager, 'generateUrlSigningJwk')
      .mockRejectedValueOnce(new Error('jwk insert failed'))
    const runMigrationsOnTenantMock = vi.mocked(migrate.runMigrationsOnTenant)
    runMigrationsOnTenantMock.mockClear()

    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/abc`,
        payload,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      expect(response.statusCode).toBe(500)
      expect(generateUrlSigningJwkSpy).toHaveBeenCalledWith('abc', expect.anything())
      expect(runMigrationsOnTenantMock).not.toHaveBeenCalled()

      await expect(multitenantKnex('tenants').where({ id: 'abc' }).first()).resolves.toBeUndefined()
      await expect(
        multitenantKnex('tenants_jwks').where({ tenant_id: 'abc' }).select('id')
      ).resolves.toEqual([])
    } finally {
      generateUrlSigningJwkSpy.mockRestore()
    }
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

  test('Update tenant config keeps changes when tenant migrations fail', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const runMigrationsOnTenantMock = vi.mocked(migrate.runMigrationsOnTenant)
    const updateTenantMigrationsStateSpy = vi.spyOn(migrate, 'updateTenantMigrationsState')
    const addTenantSpy = vi
      .spyOn(migrate.progressiveMigrations, 'addTenant')
      .mockImplementation(() => undefined)

    runMigrationsOnTenantMock.mockClear()
    updateTenantMigrationsStateSpy.mockClear()

    try {
      runMigrationsOnTenantMock.mockRejectedValueOnce(new Error('migration failed'))

      const patchResponse = await adminApp.inject({
        method: 'PATCH',
        url: `/tenants/abc`,
        payload: payload2,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      expect(patchResponse.statusCode).toBe(204)
      expect(runMigrationsOnTenantMock).toHaveBeenCalledWith(
        expect.objectContaining({
          databaseUrl: payload2.databaseUrl,
          tenantId: 'abc',
        })
      )
      expect(updateTenantMigrationsStateSpy).not.toHaveBeenCalled()
      expect(addTenantSpy).toHaveBeenCalledWith('abc')

      const getResponse = await adminApp.inject({
        method: 'GET',
        url: `/tenants/abc`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      expect(getResponse.statusCode).toBe(200)
      expect(JSON.parse(getResponse.body)).toEqual(payload2)
    } finally {
      addTenantSpy.mockRestore()
      updateTenantMigrationsStateSpy.mockRestore()
    }
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

  test('Upsert tenant config updates disableEvents and image transformation maxResolution', async () => {
    const firstPayload = {
      ...payload,
      disableEvents: ['ObjectCreated:*', 'ObjectRemoved:*'],
      features: {
        ...payload.features,
        imageTransformation: {
          ...payload.features.imageTransformation,
          maxResolution: 1024,
        },
      },
    }

    const firstPutResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: firstPayload,
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
    expect(firstGetResponse.statusCode).toBe(200)
    expect(JSON.parse(firstGetResponse.body)).toEqual(firstPayload)

    const secondPayload = {
      ...payload2,
      disableEvents: ['ObjectCreated:*'],
      features: {
        ...payload2.features,
        imageTransformation: {
          ...payload2.features.imageTransformation,
          maxResolution: 2048,
        },
      },
    }

    const secondPutResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: secondPayload,
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
    expect(secondGetResponse.statusCode).toBe(200)
    expect(JSON.parse(secondGetResponse.body)).toEqual(secondPayload)
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

    const knexTableSpy = vi.spyOn(multitenantKnex, 'table')
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
    const knexTableSpy = vi.spyOn(multitenantDbModule.multitenantKnex, 'table')
    const queryBuilder = {
      first: vi.fn().mockReturnThis(),
      where: vi.fn().mockReturnThis(),
      abortOnSignal: vi.fn().mockResolvedValue(encryptedTenant),
    }

    try {
      knexTableSpy.mockReturnValue(queryBuilder as unknown as TenantQueryBuilder)

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
      vi.doUnmock('@internal/cache')
      vi.resetModules()
      knexTableSpy.mockRestore()
    }
  })

  test('Get tenant config records one cache request per logical lookup', async () => {
    const knexTableSpy = vi.spyOn(multitenantKnex, 'table')
    const addSpy = vi.spyOn(cacheRequestsTotal, 'add')
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
      first: vi.fn().mockReturnThis(),
      where: vi.fn().mockReturnThis(),
      abortOnSignal: vi.fn().mockImplementation(() => tenantQuery.promise),
    }

    try {
      knexTableSpy.mockReturnValue(queryBuilder as unknown as TenantQueryBuilder)
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
