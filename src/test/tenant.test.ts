import { encrypt, signJWT } from '@internal/auth'
import { TENANT_CONFIG_CACHE_NAME } from '@internal/cache'
import {
  closeMultitenantPg,
  getDeleteObjectsLimit,
  jwksManager,
  multitenantPgExecutor,
  PgTenantConnection,
} from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import {
  deleteTenantConfig,
  getFeatures,
  getFileSizeLimit,
  getJwtSecret,
  getServiceKey,
  getTenantConfig,
  getTenantConfigSnapshot,
  onTenantConfigChange,
} from '@internal/database/tenant'
import * as metrics from '@internal/monitoring/metrics'
import dotenv from 'dotenv'
import * as migrate from '../internal/database/migrations/migrate'
import { adminApp } from './common'
import { assertLogicalLookupMetrics } from './utils/cache-metrics'
import { mockCreateLruCache } from './utils/cache-mock'

dotenv.config({ path: '.env.test' })

const serviceKeyPayload = { abc: 123 }
const testTenantIds = ['abc', 'cache-test-abc'] as const

const migrationVersion = Object.entries(DBMigration).sort(([_, a], [__, b]) => b - a)[0][0]

const payload = {
  anonKey: 'a',
  databaseUrl: 'b',
  databasePoolUrl: 'v',
  maxConnections: 12,
  fileSizeLimit: 1,
  deleteObjectsLimit: 1500,
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
  databaseUrl: 'f',
  databasePoolUrl: 'm',
  maxConnections: 14,
  fileSizeLimit: 2,
  deleteObjectsLimit: 2000,
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

function createEncryptedTenantRow(
  tenantId: string,
  tenantPayload: typeof payload | typeof payload2 = payload
) {
  return {
    id: tenantId,
    anon_key: encrypt(tenantPayload.anonKey),
    database_url: encrypt(tenantPayload.databaseUrl),
    database_pool_url: tenantPayload.databasePoolUrl
      ? encrypt(tenantPayload.databasePoolUrl)
      : tenantPayload.databasePoolUrl,
    max_connections: tenantPayload.maxConnections,
    file_size_limit: tenantPayload.fileSizeLimit,
    delete_objects_limit: tenantPayload.deleteObjectsLimit,
    jwt_secret: encrypt(tenantPayload.jwtSecret),
    jwks: tenantPayload.jwks,
    service_key: encrypt(tenantPayload.serviceKey),
    feature_purge_cache: tenantPayload.features.purgeCache.enabled,
    feature_image_transformation: tenantPayload.features.imageTransformation.enabled,
    feature_s3_protocol: tenantPayload.features.s3Protocol.enabled,
    feature_iceberg_catalog: tenantPayload.features.icebergCatalog.enabled,
    feature_iceberg_catalog_max_catalogs: tenantPayload.features.icebergCatalog.maxCatalogs,
    feature_iceberg_catalog_max_namespaces: tenantPayload.features.icebergCatalog.maxNamespaces,
    feature_iceberg_catalog_max_tables: tenantPayload.features.icebergCatalog.maxTables,
    feature_vector_buckets: tenantPayload.features.vectorBuckets.enabled,
    feature_vector_buckets_max_buckets: tenantPayload.features.vectorBuckets.maxBuckets,
    feature_vector_buckets_max_indexes: tenantPayload.features.vectorBuckets.maxIndexes,
    image_transformation_max_resolution: tenantPayload.features.imageTransformation.maxResolution,
    migrations_version: tenantPayload.migrationVersion,
    migrations_status: tenantPayload.migrationStatus,
    tracing_mode: tenantPayload.tracingMode,
    disable_events: tenantPayload.disableEvents,
  }
}

type TenantModule = typeof import('../internal/database/tenant')
type MultitenantPgModule = typeof import('../internal/database/multitenant-pg')

async function loadTenantModule(
  maxItems: number
): Promise<{ tenantModule: TenantModule; multitenantPgModule: MultitenantPgModule }> {
  vi.resetModules()
  mockCreateLruCache({ max: maxItems })

  return {
    tenantModule: await import('../internal/database/tenant'),
    multitenantPgModule: await import('../internal/database/multitenant-pg'),
  }
}

function mockTenantQueryResult(row: object) {
  return {
    rows: [row],
    rowCount: 1,
  } as never
}

function createTenantPoolOptions(
  tenantId: string,
  snapshot: Awaited<ReturnType<typeof getTenantConfigSnapshot>>
) {
  return {
    tenantId,
    dbUrl: snapshot.value.databasePoolUrl || snapshot.value.databaseUrl,
    isExternalPool: Boolean(snapshot.value.databasePoolUrl),
    maxConnections: snapshot.value.maxConnections ?? 10,
    configRevision: snapshot.revision,
    user: { jwt: 'jwt', payload: { role: 'authenticated' } },
    superUser: { jwt: 'service', payload: { role: 'service_role' } },
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
  await closeMultitenantPg()
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

  test.each([
    0, -1,
  ])('Get all tenant configs omits non-positive delete objects limit %i', async (deleteObjectsLimit) => {
    const tenantId = `list-delete-objects-limit-${deleteObjectsLimit}`
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      delete_objects_limit: deleteObjectsLimit,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))

    try {
      const response = await adminApp.inject({
        method: 'GET',
        url: `/tenants`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(JSON.parse(response.body)[0].deleteObjectsLimit).toBeUndefined()
    } finally {
      querySpy.mockRestore()
    }
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
    await expect(getDeleteObjectsLimit('abc')).resolves.toBe(payload.deleteObjectsLimit)
    await expect(getFeatures('abc')).resolves.toEqual(payload.features)
  })

  test('PATCH refreshes local tenant config changes before the notify cache path', async () => {
    const createResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(createResponse.statusCode).toBe(201)

    await getTenantConfig('abc')
    const reconcileSpy = vi.spyOn(PgTenantConnection.poolManager, 'reconcileExisting')

    try {
      const response = await adminApp.inject({
        method: 'PATCH',
        url: `/tenants/abc`,
        payload: {
          databasePoolUrl: 'postgres://pool.example.test/postgres',
        },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(204)

      await vi.waitFor(() => {
        expect(reconcileSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            tenantId: 'abc',
            dbUrl: 'postgres://pool.example.test/postgres',
            isExternalPool: true,
            configRevision: expect.any(Number),
          })
        )
      })
    } finally {
      reconcileSpy.mockRestore()
    }
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
    let isolatedApp: typeof adminApp | undefined

    try {
      vi.resetModules()
      const { default: createApp } = await import('../admin-app')
      isolatedApp = createApp({})

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
    } finally {
      await isolatedApp?.close()

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

      const tenant = await multitenantPgExecutor.query({
        text: 'SELECT id FROM tenants WHERE id = $1 LIMIT 1',
        values: ['abc'],
      })
      const jwks = await multitenantPgExecutor.query({
        text: 'SELECT id FROM tenants_jwks WHERE tenant_id = $1',
        values: ['abc'],
      })

      expect(tenant.rows[0]).toBeUndefined()
      expect(jwks.rows).toEqual([])
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

  test('Update tenant config partially can disable globally enabled icebergCatalog', async () => {
    const createResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(createResponse.statusCode).toBe(201)

    const patchResponse = await adminApp.inject({
      method: 'PATCH',
      url: `/tenants/abc`,
      payload: {
        features: {
          icebergCatalog: {
            enabled: false,
          },
        },
      },
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
    expect(JSON.parse(getResponse.body).features.icebergCatalog).toEqual({
      ...payload.features.icebergCatalog,
      enabled: false,
    })

    deleteTenantConfig('abc')
    await expect(getTenantConfig('abc')).resolves.toMatchObject({
      features: {
        icebergCatalog: {
          ...payload.features.icebergCatalog,
          enabled: false,
        },
      },
    })
  })

  test('Update tenant config partially can disable globally enabled vectorBuckets', async () => {
    const createResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(createResponse.statusCode).toBe(201)

    const patchResponse = await adminApp.inject({
      method: 'PATCH',
      url: `/tenants/abc`,
      payload: {
        features: {
          vectorBuckets: {
            enabled: false,
          },
        },
      },
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
    expect(JSON.parse(getResponse.body).features.vectorBuckets).toEqual({
      ...payload.features.vectorBuckets,
      enabled: false,
    })

    deleteTenantConfig('abc')
    await expect(getTenantConfig('abc')).resolves.toMatchObject({
      features: {
        vectorBuckets: {
          ...payload.features.vectorBuckets,
          enabled: false,
        },
      },
    })
  })

  test('Tenant config maxConnections nullish transitions do not retire cached pg pool', async () => {
    const tenantId = 'pool-max-connections-nullish-change'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_pool_url: null,
      max_connections: null,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...encryptedTenant,
          max_connections: undefined,
        })
      )
    const retireSpy = vi
      .spyOn(PgTenantConnection.poolManager, 'retire')
      .mockResolvedValue(undefined)

    try {
      const cachedConfig = await getTenantConfig(tenantId)
      ;(cachedConfig as { maxConnections?: number | null }).maxConnections = null

      await onTenantConfigChange(tenantId)

      expect(retireSpy).not.toHaveBeenCalled()
      expect(querySpy).toHaveBeenCalledTimes(2)
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      retireSpy.mockRestore()
    }
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

  test('PUT clears tenant databasePoolUrl when set to null', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const putResponse = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: { ...payload, databasePoolUrl: null },
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
    expect(JSON.parse(getResponse.body).databasePoolUrl).toBeNull()
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

  test.each([
    0, -1,
  ])('Get delete objects limit returns undefined for non-positive stored value %i', async (deleteObjectsLimit) => {
    const tenantId = `delete-objects-limit-${deleteObjectsLimit}`
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      delete_objects_limit: deleteObjectsLimit,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))

    try {
      await expect(getDeleteObjectsLimit(tenantId)).resolves.toBeUndefined()
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
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

    const querySpy = vi.spyOn(multitenantPgExecutor, 'query')
    try {
      await getTenantConfig(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(1)

      const results = await Promise.all([
        getTenantConfig(tenantId),
        getTenantConfig(tenantId),
        getTenantConfig(tenantId),
      ])
      expect(querySpy).toHaveBeenCalledTimes(1)
      results.forEach((result, i) => expect(result).toEqual(results[i === 0 ? 1 : 0]))

      await adminApp.inject({
        method: 'DELETE',
        url: `/tenants/${tenantId}`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
    } finally {
      querySpy.mockRestore()
    }
  })

  test('Tenant config invalidation redirects detached pool and config consumers', async () => {
    const tenantId = 'cache-invalidation-generation'
    const oldLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const newLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const oldTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://old.example.test/postgres'),
      database_pool_url: null,
    }
    const newTenant = {
      ...oldTenant,
      database_url: encrypt('postgres://new.example.test/postgres'),
      jwt_secret: encrypt(payload2.jwtSecret),
      feature_purge_cache: payload2.features.purgeCache.enabled,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(oldLoad.promise as never)
      .mockReturnValueOnce(newLoad.promise as never)

    try {
      const first = getTenantConfigSnapshot(tenantId)
      const firstConfigConsumer = getTenantConfig(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(1))

      deleteTenantConfig(tenantId)
      const second = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(2))

      newLoad.resolve(mockTenantQueryResult(newTenant))
      const secondSnapshot = await second
      expect(secondSnapshot).toMatchObject({
        value: { databaseUrl: 'postgres://new.example.test/postgres' },
      })

      oldLoad.resolve(mockTenantQueryResult(oldTenant))
      const firstSnapshot = await first
      expect(firstSnapshot).toBe(secondSnapshot)
      await expect(firstConfigConsumer).resolves.toMatchObject({
        jwtSecret: payload2.jwtSecret,
        features: {
          purgeCache: {
            enabled: payload2.features.purgeCache.enabled,
          },
        },
      })

      const resident = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, firstSnapshot)
      )
      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://new.example.test/postgres',
        configRevision: secondSnapshot.revision,
      })

      await expect(getTenantConfigSnapshot(tenantId)).resolves.toMatchObject({
        value: { databaseUrl: 'postgres://new.example.test/postgres' },
      })
      expect(querySpy).toHaveBeenCalledTimes(2)
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Tenant notification reconciles a detached load that settles before its replacement', async () => {
    const tenantId = 'detached-load-before-replacement'
    const oldLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const newLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const oldTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://old.example.test/postgres'),
      database_pool_url: null,
    }
    const newTenant = {
      ...oldTenant,
      database_url: encrypt('postgres://new.example.test/postgres'),
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(oldLoad.promise as never)
      .mockReturnValueOnce(newLoad.promise as never)

    try {
      const detached = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(1))

      const notification = onTenantConfigChange(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(2))

      oldLoad.resolve(mockTenantQueryResult(oldTenant))
      const oldSnapshot = await detached
      expect(oldSnapshot).toMatchObject({
        value: { databaseUrl: 'postgres://old.example.test/postgres' },
      })

      const resident = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, oldSnapshot)
      )
      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://old.example.test/postgres',
        configRevision: oldSnapshot.revision,
      })

      newLoad.resolve(mockTenantQueryResult(newTenant))
      await notification

      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://new.example.test/postgres',
        configRevision: expect.any(Number),
      })
      expect(
        (resident as unknown as { options: { configRevision: number } }).options.configRevision
      ).toBeGreaterThan(oldSnapshot.revision)
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Tenant config redirects a detached failure to the active replacement', async () => {
    const tenantId = 'detached-failure-before-replacement'
    const missingLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const replacementLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(missingLoad.promise as never)
      .mockReturnValueOnce(replacementLoad.promise as never)
    const recordSpy = vi.spyOn(metrics, 'recordCacheRequest')

    try {
      const detached = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(1))

      deleteTenantConfig(tenantId)
      const replacement = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(2))

      const detachedResult = detached.then(
        (value) => ({ status: 'fulfilled' as const, value }),
        (reason) => ({ status: 'rejected' as const, reason })
      )
      missingLoad.resolve({ rows: [], rowCount: 0 } as never)
      await new Promise<void>((resolve) => setImmediate(resolve))
      expect(
        recordSpy.mock.calls.filter(([cacheName]) => cacheName === TENANT_CONFIG_CACHE_NAME)
      ).toEqual([
        [TENANT_CONFIG_CACHE_NAME, 'miss'],
        [TENANT_CONFIG_CACHE_NAME, 'miss'],
      ])

      replacementLoad.resolve(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://replacement.example.test/postgres'),
          database_pool_url: null,
        })
      )

      const replacementSnapshot = await replacement
      await expect(detachedResult).resolves.toEqual({
        status: 'fulfilled',
        value: replacementSnapshot,
      })
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      recordSpy.mockRestore()
    }
  })

  test('Tenant config redirects a detached failure to a committed replacement', async () => {
    const tenantId = 'detached-failure-after-replacement'
    const missingLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const replacementLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(missingLoad.promise as never)
      .mockReturnValueOnce(replacementLoad.promise as never)

    try {
      const detached = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(1))

      deleteTenantConfig(tenantId)
      const replacement = getTenantConfigSnapshot(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(2))

      replacementLoad.resolve(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://committed.example.test/postgres'),
          database_pool_url: null,
        })
      )
      const replacementSnapshot = await replacement

      missingLoad.resolve({ rows: [], rowCount: 0 } as never)

      await expect(detached).resolves.toBe(replacementSnapshot)
      expect(querySpy).toHaveBeenCalledTimes(2)
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
  })

  test('Tenant config bounds failure handoffs during generation churn', async () => {
    const tenantId = 'bounded-detached-failure-handoffs'
    const loads = Array.from({ length: 18 }, () =>
      Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    )
    let loadIndex = 0
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockImplementation(() => loads[loadIndex++].promise as never)

    try {
      const detachedResult = getTenantConfigSnapshot(tenantId).then(
        (value) => ({ status: 'fulfilled' as const, value }),
        (reason) => ({ status: 'rejected' as const, reason })
      )
      const replacements: Array<Promise<unknown>> = []

      for (let generation = 1; generation < loads.length; generation++) {
        deleteTenantConfig(tenantId)
        replacements.push(getTenantConfigSnapshot(tenantId))
        await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(generation + 1))

        loads[generation - 1].resolve({ rows: [], rowCount: 0 } as never)
        await new Promise<void>((resolve) => setImmediate(resolve))
      }

      await expect(detachedResult).resolves.toMatchObject({
        status: 'rejected',
        reason: expect.objectContaining({
          message: 'Could not load current tenant configuration after 16 attempts',
        }),
      })

      loads.at(-1)?.resolve(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://stable.example.test/postgres'),
          database_pool_url: null,
        })
      )
      await expect(Promise.all(replacements)).resolves.toHaveLength(17)
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
  })

  test('Overlapping tenant notifications synchronously chain replacement generations', async () => {
    const tenantId = 'overlapping-notification-generations'
    const firstLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const secondLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const thirdLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const baseTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_pool_url: null,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(firstLoad.promise as never)
      .mockReturnValueOnce(secondLoad.promise as never)
      .mockReturnValueOnce(thirdLoad.promise as never)

    try {
      const first = getTenantConfigSnapshot(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(1)

      const firstNotification = onTenantConfigChange(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(2)

      const secondNotification = onTenantConfigChange(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(3)

      thirdLoad.resolve(
        mockTenantQueryResult({
          ...baseTenant,
          database_url: encrypt('postgres://third.example.test/postgres'),
        })
      )
      await secondNotification

      secondLoad.resolve(
        mockTenantQueryResult({
          ...baseTenant,
          database_url: encrypt('postgres://second.example.test/postgres'),
        })
      )
      await firstNotification

      firstLoad.resolve(
        mockTenantQueryResult({
          ...baseTenant,
          database_url: encrypt('postgres://first.example.test/postgres'),
        })
      )
      await first

      await expect(getTenantConfigSnapshot(tenantId)).resolves.toMatchObject({
        value: { databaseUrl: 'postgres://third.example.test/postgres' },
      })
      expect(querySpy).toHaveBeenCalledTimes(3)
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
  })

  test('Tenant notification skips control-plane refresh without local interest', async () => {
    const tenantId = 'notification-without-local-interest'
    const querySpy = vi.spyOn(multitenantPgExecutor, 'query')

    try {
      await onTenantConfigChange(tenantId)
      expect(querySpy).not.toHaveBeenCalled()
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
  })

  test('Tenant notification does not serve a stale secret after refresh failure', async () => {
    const tenantId = 'notification-secret-rotation'
    const oldTenant = createEncryptedTenantRow(tenantId, payload)
    const newTenant = createEncryptedTenantRow(tenantId, payload2)
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(oldTenant))
      .mockRejectedValueOnce(new Error('control plane unavailable'))
      .mockResolvedValueOnce(mockTenantQueryResult(newTenant))

    try {
      await expect(getTenantConfig(tenantId)).resolves.toMatchObject({
        serviceKey: payload.serviceKey,
      })

      await onTenantConfigChange(tenantId)

      await expect(getTenantConfig(tenantId)).resolves.toMatchObject({
        serviceKey: payload2.serviceKey,
      })
      expect(querySpy).toHaveBeenCalledTimes(3)
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
    }
  })

  test('Repeated single-tenant JWT config reads reuse the empty JWKS object', async () => {
    const first = await getJwtSecret('unused')
    const second = await getJwtSecret('unused')

    expect(second.jwks).toBe(first.jwks)
    expect(Object.isFrozen(first.jwks)).toBe(true)
    expect(Object.isFrozen(first.jwks.keys)).toBe(true)
  })

  test('Get tenant config evicts cold tenants from cache', async () => {
    const tenantIds = ['cache-eviction-1', 'cache-eviction-2', 'cache-eviction-3']
    const encryptedTenant = createEncryptedTenantRow(tenantIds[0])

    const { tenantModule, multitenantPgModule } = await loadTenantModule(2)
    const querySpy = vi
      .spyOn(multitenantPgModule.multitenantPgExecutor, 'query')
      .mockResolvedValue(mockTenantQueryResult(encryptedTenant))

    try {
      for (const tenantId of tenantIds) {
        await tenantModule.getTenantConfig(tenantId)
      }

      expect(querySpy).toHaveBeenCalledTimes(tenantIds.length)

      await tenantModule.getTenantConfig(tenantIds[0])

      expect(querySpy).toHaveBeenCalledTimes(tenantIds.length + 1)
    } finally {
      tenantIds.forEach((tenantId) => {
        tenantModule.deleteTenantConfig(tenantId)
      })
      vi.doUnmock('@internal/cache')
      vi.resetModules()
      querySpy.mockRestore()
    }
  })

  test('First multitenant config generation starts at revision one', async () => {
    const tenantId = 'first-config-revision'
    const encryptedTenant = createEncryptedTenantRow(tenantId)
    const { tenantModule, multitenantPgModule } = await loadTenantModule(2)
    const querySpy = vi
      .spyOn(multitenantPgModule.multitenantPgExecutor, 'query')
      .mockResolvedValue(mockTenantQueryResult(encryptedTenant))

    try {
      await expect(tenantModule.getTenantConfigSnapshot(tenantId)).resolves.toMatchObject({
        revision: 1,
      })
    } finally {
      tenantModule.deleteTenantConfig(tenantId)
      vi.doUnmock('@internal/cache')
      vi.resetModules()
      querySpy.mockRestore()
    }
  })

  test('Tenant config maxConnections change reconciles the resident pg pool', async () => {
    const tenantId = 'pool-max-connections-change'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      max_connections: 20,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...encryptedTenant,
          max_connections: 40,
        })
      )
    const reconcileSpy = vi.spyOn(PgTenantConnection.poolManager, 'reconcileExisting')

    try {
      await getTenantConfig(tenantId)
      await onTenantConfigChange(tenantId)

      expect(reconcileSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          tenantId,
          maxConnections: 40,
          configRevision: expect.any(Number),
        })
      )
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      reconcileSpy.mockRestore()
    }
  })

  test('Tenant config databaseUrl change reconciles the resident pg pool', async () => {
    const tenantId = 'pool-dburl-change'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://old-host'),
      database_pool_url: null,
      max_connections: 20,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...encryptedTenant,
          database_url: encrypt('postgres://new-host'),
        })
      )
    const reconcileSpy = vi.spyOn(PgTenantConnection.poolManager, 'reconcileExisting')

    try {
      await getTenantConfig(tenantId)
      await onTenantConfigChange(tenantId)

      expect(reconcileSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          tenantId,
          dbUrl: 'postgres://new-host',
          isExternalPool: false,
          configRevision: expect.any(Number),
        })
      )
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      reconcileSpy.mockRestore()
    }
  })

  test('Tenant config databasePoolUrl change reconciles the resident pg pool', async () => {
    const tenantId = 'pool-dbpoolurl-change'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_pool_url: encrypt('postgres://old-pooler'),
      max_connections: 20,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...encryptedTenant,
          database_pool_url: encrypt('postgres://new-pooler'),
        })
      )
    const reconcileSpy = vi.spyOn(PgTenantConnection.poolManager, 'reconcileExisting')

    try {
      await getTenantConfig(tenantId)
      await onTenantConfigChange(tenantId)

      expect(reconcileSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          tenantId,
          dbUrl: 'postgres://new-pooler',
          isExternalPool: true,
          configRevision: expect.any(Number),
        })
      )
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      reconcileSpy.mockRestore()
    }
  })

  test('Tenant config dbUrl and maxConnections change reconcile together', async () => {
    const tenantId = 'pool-dburl-and-max-change'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://old-host'),
      database_pool_url: null,
      max_connections: 20,
    }
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(encryptedTenant))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...encryptedTenant,
          database_url: encrypt('postgres://new-host'),
          max_connections: 40,
        })
      )
    const reconcileSpy = vi.spyOn(PgTenantConnection.poolManager, 'reconcileExisting')

    try {
      await getTenantConfig(tenantId)
      await onTenantConfigChange(tenantId)

      expect(reconcileSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          tenantId,
          dbUrl: 'postgres://new-host',
          maxConnections: 40,
          configRevision: expect.any(Number),
        })
      )
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      reconcileSpy.mockRestore()
    }
  })

  test('Tenant notification reconciles a resident pool without an old config snapshot', async () => {
    const tenantId = 'pool-reconcile-without-config-baseline'
    const resident = PgTenantConnection.poolManager.getPool({
      tenantId,
      dbUrl: 'postgres://old.example.test/postgres',
      isExternalPool: false,
      maxConnections: 10,
      configRevision: 0,
      user: { jwt: 'jwt', payload: { role: 'authenticated' } },
      superUser: { jwt: 'service', payload: { role: 'service_role' } },
    })
    const querySpy = vi.spyOn(multitenantPgExecutor, 'query').mockResolvedValueOnce(
      mockTenantQueryResult({
        ...createEncryptedTenantRow(tenantId),
        database_url: encrypt('postgres://new.example.test/postgres'),
        database_pool_url: null,
      })
    )

    try {
      await onTenantConfigChange(tenantId)

      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://new.example.test/postgres',
        isExternalPool: false,
        configRevision: expect.any(Number),
      })
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Tenant notification reconciles a stale pool created during its refresh', async () => {
    const tenantId = 'pool-created-during-notification-refresh'
    const oldTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://old.example.test/postgres'),
      database_pool_url: null,
    }
    const newTenant = {
      ...oldTenant,
      database_url: encrypt('postgres://new.example.test/postgres'),
    }
    const refresh = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce(mockTenantQueryResult(oldTenant))
      .mockReturnValueOnce(refresh.promise as never)

    try {
      const oldSnapshot = await getTenantConfigSnapshot(tenantId)
      const notification = onTenantConfigChange(tenantId)
      await vi.waitFor(() => expect(querySpy).toHaveBeenCalledTimes(2))

      const resident = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, oldSnapshot)
      )

      refresh.resolve(mockTenantQueryResult(newTenant))
      await notification

      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://new.example.test/postgres',
        configRevision: expect.any(Number),
      })
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Transient notification refresh failure preserves the pool and retries config later', async () => {
    const tenantId = 'pool-preserved-on-refresh-failure'
    const resident = PgTenantConnection.poolManager.getPool({
      tenantId,
      dbUrl: 'postgres://old.example.test/postgres',
      isExternalPool: false,
      maxConnections: 10,
      configRevision: 0,
      user: { jwt: 'jwt', payload: { role: 'authenticated' } },
      superUser: { jwt: 'service', payload: { role: 'service_role' } },
    })
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockRejectedValueOnce(new Error('control plane unavailable'))
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://new.example.test/postgres'),
          database_pool_url: null,
        })
      )

    try {
      await onTenantConfigChange(tenantId)

      expect(PgTenantConnection.poolManager.hasPool(tenantId)).toBe(true)

      const snapshot = await getTenantConfigSnapshot(tenantId)
      const reconciled = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, snapshot)
      )

      expect(reconciled).toBe(resident)
      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://new.example.test/postgres',
      })
      expect(querySpy).toHaveBeenCalledTimes(2)
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Confirmed deletion removes and retires the old strategy without poisoning recreation', async () => {
    const tenantId = 'pool-delete-recreate'
    const retained = PgTenantConnection.poolManager.getPool({
      tenantId,
      dbUrl: 'postgres://old.example.test/postgres',
      isExternalPool: false,
      maxConnections: 10,
      configRevision: 0,
      user: { jwt: 'jwt', payload: { role: 'authenticated' } },
      superUser: { jwt: 'service', payload: { role: 'service_role' } },
    })
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockResolvedValueOnce({ rows: [], rowCount: 0 } as never)
      .mockResolvedValueOnce(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://recreated.example.test/postgres'),
          database_pool_url: null,
        })
      )

    try {
      await onTenantConfigChange(tenantId)

      expect(PgTenantConnection.poolManager.hasPool(tenantId)).toBe(false)
      expect(() => retained.acquire()).toThrow(expect.objectContaining({ code: 'TenantNotFound' }))

      const recreatedSnapshot = await getTenantConfigSnapshot(tenantId)
      const recreated = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, recreatedSnapshot)
      )

      expect(recreated).not.toBe(retained)
      expect((recreated as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://recreated.example.test/postgres',
      })
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('An obsolete missing generation does not retire a recreated tenant strategy', async () => {
    const tenantId = 'pool-obsolete-delete-after-recreate'
    const missingLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const recreatedLoad = Promise.withResolvers<ReturnType<typeof mockTenantQueryResult>>()
    const resident = PgTenantConnection.poolManager.getPool({
      tenantId,
      dbUrl: 'postgres://old.example.test/postgres',
      isExternalPool: false,
      maxConnections: 10,
      configRevision: 0,
      user: { jwt: 'jwt', payload: { role: 'authenticated' } },
      superUser: { jwt: 'service', payload: { role: 'service_role' } },
    })
    const querySpy = vi
      .spyOn(multitenantPgExecutor, 'query')
      .mockReturnValueOnce(missingLoad.promise as never)
      .mockReturnValueOnce(recreatedLoad.promise as never)

    try {
      const obsoleteDeletion = onTenantConfigChange(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(1)

      const recreation = onTenantConfigChange(tenantId)
      expect(querySpy).toHaveBeenCalledTimes(2)

      recreatedLoad.resolve(
        mockTenantQueryResult({
          ...createEncryptedTenantRow(tenantId),
          database_url: encrypt('postgres://recreated.example.test/postgres'),
          database_pool_url: null,
        })
      )
      await recreation

      missingLoad.resolve({ rows: [], rowCount: 0 } as never)
      await obsoleteDeletion

      const recreatedSnapshot = await getTenantConfigSnapshot(tenantId)
      const current = PgTenantConnection.poolManager.getPool(
        createTenantPoolOptions(tenantId, recreatedSnapshot)
      )

      expect(current).toBe(resident)
      expect((resident as unknown as { options: Record<string, unknown> }).options).toMatchObject({
        dbUrl: 'postgres://recreated.example.test/postgres',
        configRevision: recreatedSnapshot.revision,
      })
      expect(querySpy).toHaveBeenCalledTimes(2)
    } finally {
      deleteTenantConfig(tenantId)
      await PgTenantConnection.poolManager.retire(tenantId, new Error('test cleanup'))
      querySpy.mockRestore()
    }
  })

  test('Get tenant config records one cache request per logical lookup', async () => {
    const querySpy = vi.spyOn(multitenantPgExecutor, 'query')
    const recordSpy = vi.spyOn(metrics, 'recordCacheRequest')
    const tenantId = 'cache-metrics-lookup'
    const encryptedTenant = {
      ...createEncryptedTenantRow(tenantId),
      database_url: encrypt('postgres://tenant'),
    }

    const tenantQuery = Promise.withResolvers<never>()

    try {
      querySpy.mockImplementation(() => tenantQuery.promise)
      await assertLogicalLookupMetrics({
        recordSpy,
        backendCallSpy: querySpy,
        cacheName: TENANT_CONFIG_CACHE_NAME,
        startLookups: () => [
          getTenantConfig(tenantId),
          getTenantConfig(tenantId),
          getTenantConfig(tenantId),
        ],
        resolveBackend: () => tenantQuery.resolve(mockTenantQueryResult(encryptedTenant)),
        assertCachedHit: async () => {
          await expect(getTenantConfig(tenantId)).resolves.toMatchObject({
            databaseUrl: 'postgres://tenant',
          })
        },
      })
    } finally {
      deleteTenantConfig(tenantId)
      querySpy.mockRestore()
      recordSpy.mockRestore()
    }
  })
})
