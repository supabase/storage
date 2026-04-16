vi.hoisted(() => {
  process.env.PG_QUEUE_ENABLE = 'true'
  process.env.MULTI_TENANT = 'true'
  process.env.IS_MULTITENANT = 'true'
})

import { getConfig, mergeConfig } from '../config'

const { multitenantDatabaseUrl } = getConfig()
mergeConfig({
  pgQueueEnable: true,
  isMultitenant: true,
})

import { encrypt, signJWT } from '@internal/auth'
import { TENANTS_JWKS_UPDATE_CHANNEL } from '@internal/auth/jwks/channels'
import { UrlSigningJwkGenerator } from '@internal/auth/jwks/generator'
import { JWKSManagerStoreKnex } from '@internal/auth/jwks/store-knex'
import { TENANT_JWKS_CACHE_NAME } from '@internal/cache'
import {
  deleteTenantConfig,
  getJwtSecret,
  jwksManager,
  listenForTenantUpdate,
} from '@internal/database'
import { cacheRequestsTotal } from '@internal/monitoring/metrics'
import { PostgresPubSub } from '@internal/pubsub'
import dotenv from 'dotenv'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { adminApp, mockQueue } from './common'
import { createMockKnexReturning } from './mocks/knex-mock'
import { assertLogicalLookupMetrics } from './utils/cache-metrics'
import { mockCreateLruCache } from './utils/cache-mock'
import { waitForEventually } from './utils/promise'

dotenv.config({ path: '.env.test' })

// Keep helper-level waits short so helper errors surface first.
const TENANT_JWKS_HELPER_TIMEOUT_MS = 4000
const tenantId = 'abc123'

const testJwks = {
  oct: {
    kty: 'oct',
    k: 'nrRW40eXW1wEzzqhsyIRieFZNrUA59sowrTTWLzPJks',
  },
  rsa: {
    kty: 'RSA',
    n: '21fa-bq1RLWAuHcpX_XGwGoroJxjFvqvlB_UStb-hT4aA-5DPwsHZgPGJQGvy7vYoKvF2e9ajtK5tFr0qGXX2gOlobC2sCDUJRowhlnKHmnZcVUQB3J6TQeZVGvT1_nG1OHyPjUO1BAgUhF3MQCnKuNel1MXRm2D-XRHFS1NN3-Xn2mDljWUN8tvzo51AormeRrdb-x_-I28wZkamp6mAiSZMWj_fJXHwUAYSB3ZHBH4Ay6j4Prs4R9gtCqguBO6hOaGLJh7trP-mS9pPEp0AdbKK5w64aZaQz65BMj5RAlh36VUkJkKFE6w79hS9DSnhF-qxCveYW8yT6OEHjzVCQ',
    e: 'AQAB',
    alg: 'RS256',
  },
  ec: {
    kty: 'EC',
    x: 'lrQU8Pt8cp47ctno-Kr9RvCO_6haU3au9fWE-X-XnR0',
    y: 'FCyiClHRp03d7rHkeugpo7qH4DirJBPoXv8GwPLWSNM',
    crv: 'P-256',
    alg: 'ES256',
  },
  okp: {
    crv: 'Ed25519',
    x: 'D-23h3lUY63Vjofh51cwFwMG5KfVDV10p0QLu2xGmXA',
    kty: 'OKP',
    alg: 'EdDSA',
  },
}

const pubSub = new PostgresPubSub(multitenantDatabaseUrl!)

type DatabaseModule = typeof import('../internal/database')
type JwksModule = typeof import('../internal/auth/jwks')

async function loadJwksModules(
  maxItems: number
): Promise<{ databaseModule: DatabaseModule; jwksModule: JwksModule }> {
  vi.resetModules()

  const configModule = await import('../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    pgQueueEnable: true,
    isMultitenant: true,
  })

  mockCreateLruCache({ max: maxItems })

  return {
    databaseModule: await import('../internal/database'),
    jwksModule: await import('../internal/auth/jwks'),
  }
}

// returns a promise that resolves the next time the jwk cache is invalidated
function createJwkConfigChangeAwaiter(
  expectedCacheKey = tenantId,
  timeoutMs = TENANT_JWKS_HELPER_TIMEOUT_MS
): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(() => {
      pubSub.subscriber.notifications.removeListener(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
      reject(
        new Error(
          `Timed out after ${timeoutMs}ms waiting for ${TENANTS_JWKS_UPDATE_CHANNEL}:${expectedCacheKey}`
        )
      )
    }, timeoutMs)

    const onNotification = (cacheKey: string) => {
      if (cacheKey !== expectedCacheKey) {
        return
      }

      clearTimeout(timeout)
      pubSub.subscriber.notifications.removeListener(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
      resolve(cacheKey)
    }

    pubSub.subscriber.notifications.on(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
  })
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  await pubSub.start()
  await listenForTenantUpdate(pubSub)
  vi.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
})

beforeEach(async () => {
  const jwtSecret = 'zzzzzzzzzzz'
  const serviceKey = await signJWT({}, jwtSecret, 100)
  await adminApp.inject({
    method: 'POST',
    url: `/tenants/${tenantId}`,
    payload: {
      anonKey: 'aaaa',
      databaseUrl: 'bbbb',
      jwtSecret,
      serviceKey,
    },
    headers: {
      apikey: process.env.ADMIN_API_KEYS,
    },
  })
})

afterEach(async () => {
  await adminApp.inject({
    method: 'DELETE',
    url: `/tenants/${tenantId}`,
    headers: {
      apikey: process.env.ADMIN_API_KEYS,
    },
  })
})

afterAll(async () => {
  await adminApp.close()
  await pubSub.close()
  await multitenantKnex.destroy()
})

describe('Tenant jwks configs', () => {
  test('JWK change awaiter ignores unrelated tenant notifications', async () => {
    const expectedTenantId = 'expected-jwks-tenant'
    const awaiter = createJwkConfigChangeAwaiter(expectedTenantId)
    let resolved = false

    void awaiter.then(() => {
      resolved = true
    })

    pubSub.subscriber.notifications.emit(TENANTS_JWKS_UPDATE_CHANNEL, 'other-jwks-tenant')
    await new Promise((resolve) => setImmediate(resolve))

    expect(resolved).toBe(false)

    pubSub.subscriber.notifications.emit(TENANTS_JWKS_UPDATE_CHANNEL, expectedTenantId)

    await expect(awaiter).resolves.toBe(expectedTenantId)
  })

  test('Add jwk without jwk in payload', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/jwks`,
      payload: { kind: 'abc' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('Add jwk with unknown kty', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/jwks`,
      payload: { jwk: { kty: 'nonsense' }, kind: 'abc' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test(`Add jwk with invalid characters in kind`, async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/jwks`,
      payload: { jwk: testJwks.oct, kind: 'invalid_chars_in_kind' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test(`Add jwk with kind that exceeds max length`, async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/jwks`,
      payload: { jwk: testJwks.oct, kind: 'z'.repeat(51) },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  Object.entries(testJwks).forEach(([type, jwk]) => {
    test(`Add ${type} jwk`, async () => {
      const kind = 'testing123'
      const { keys: keysBefore } = await jwksManager.getJwksTenantConfig(tenantId)
      const configAwaiter = createJwkConfigChangeAwaiter()
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/${tenantId}/jwks`,
        payload: { jwk, kind },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })

      expect(response.statusCode).toBe(201)
      const data = response.json<{ kid: string }>()
      expect(data.kid).toBeTruthy()
      expect(data.kid.startsWith(kind)).toBe(true)

      await expect(configAwaiter).resolves.toBe(tenantId)

      const config = await waitForEventually(
        () => jwksManager.getJwksTenantConfig(tenantId),
        (value) => value.keys.some((key) => key.kid === data.kid),
        `tenant ${tenantId} JWKS to include ${data.kid}`
      )
      expect(config.keys.length - keysBefore.length).toBe(1)
      expect(config.keys.find((v) => v.kid === data.kid)).toBeTruthy()
    })

    test(`Add ${type} jwk via tenant patch (legacy)`, async () => {
      const secretBeforePatch = await getJwtSecret(tenantId)

      const patchResponse = await adminApp.inject({
        method: 'PATCH',
        url: `/tenants/${tenantId}`,
        payload: {
          jwks: { keys: [jwk] },
        },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(patchResponse.statusCode).toBe(204)

      deleteTenantConfig(tenantId)

      const secretAfterPatch = await getJwtSecret(tenantId)
      const secretAfterSecondRead = await getJwtSecret(tenantId)
      const { jwks } = secretAfterPatch

      expect(jwks.keys.length).toBe(2)
      expect(jwks.keys[1]).toEqual(jwk)
      expect(secretAfterPatch.secret).toBe(secretBeforePatch.secret)
      expect(secretAfterSecondRead.jwks).toBe(secretAfterPatch.jwks)
      expect(secretAfterSecondRead.jwks.keys).toEqual(jwks.keys)
    })

    test(`Add ${type} jwk with missing data`, async () => {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/${tenantId}/jwks`,
        payload: { jwk: { kty: jwk.kty }, kind: 'abc' },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(400)
    })

    if (type !== 'oct') {
      test(`Add ${type} jwk with private data`, async () => {
        const response = await adminApp.inject({
          method: 'POST',
          url: `/tenants/${tenantId}/jwks`,
          payload: { jwk: { ...jwk, d: 'zzzzzzz' }, kind: 'testing' },
          headers: {
            apikey: process.env.ADMIN_API_KEYS,
          },
        })
        expect(response.statusCode).toBe(400)
      })
    }
  })

  test('Update jwk (deactivate and reactivate)', async () => {
    let configAwaiter = createJwkConfigChangeAwaiter()

    let config = await jwksManager.getJwksTenantConfig(tenantId)
    expect(config.keys.length).toBe(1)
    const { kid } = config.keys[0]

    let response = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/${kid}`,
      payload: { active: false },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    let data = response.json<{ result: boolean }>()
    expect(data.result).toBe(true)

    await expect(configAwaiter).resolves.toBe(tenantId)

    config = await waitForEventually(
      () => jwksManager.getJwksTenantConfig(tenantId),
      (value) => value.keys.length === 0,
      `tenant ${tenantId} JWKS to clear after deactivation`
    )
    expect(config.keys.length).toBe(0)

    configAwaiter = createJwkConfigChangeAwaiter()
    response = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/${kid}`,
      payload: { active: true },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    data = response.json<{ result: boolean }>()
    expect(data.result).toBe(true)

    await expect(configAwaiter).resolves.toBe(tenantId)

    const config2 = await waitForEventually(
      () => jwksManager.getJwksTenantConfig(tenantId),
      (value) => value.keys.some((key) => key.kid === kid),
      `tenant ${tenantId} JWKS to restore ${kid}`
    )
    expect(config2.keys.length).toBe(1)
    expect(config2.keys[0]).toMatchObject({ kid })
  })

  test('Update unknown jwk', async () => {
    const response = await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/fake-nonsense_186e5fae-7b67-4939-b425-bbd649844163`,
      payload: { active: false },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    const data = response.json<{ result: boolean }>()
    expect(data.result).toBe(false)
  })

  test('Config always retrieves concurrent requests from cache', async () => {
    const listActiveSpy = vi.spyOn(jwksManager['storage'], 'listActive')
    try {
      const results = await Promise.all([
        jwksManager.getJwksTenantConfig(tenantId),
        jwksManager.getJwksTenantConfig(tenantId),
        jwksManager.getJwksTenantConfig(tenantId),
      ])
      expect(listActiveSpy).toHaveBeenCalledTimes(1)
      results.forEach((result, i) => expect(result).toEqual(results[i === 0 ? 1 : 0]))
    } finally {
      listActiveSpy.mockRestore()
    }
  })

  test('Config evicts cold tenants from cache', async () => {
    const tenantIds = ['jwks-cache-eviction-1', 'jwks-cache-eviction-2', 'jwks-cache-eviction-3']
    const encryptedJwk = {
      id: 'cache-eviction',
      kind: 'storage-url-signing-key',
      content: encrypt(JSON.stringify({ kty: 'oct', k: 'bounded-cache-test-key' })),
    }

    const { databaseModule, jwksModule } = await loadJwksModules(2)
    const listActiveSpy = vi.spyOn(databaseModule.jwksManager['storage'], 'listActive')

    try {
      listActiveSpy.mockImplementation(async () => {
        return [encryptedJwk]
      })

      for (const tenantId of tenantIds) {
        await databaseModule.jwksManager.getJwksTenantConfig(tenantId)
      }

      expect(listActiveSpy).toHaveBeenCalledTimes(tenantIds.length)

      await databaseModule.jwksManager.getJwksTenantConfig(tenantIds[0])

      expect(listActiveSpy).toHaveBeenCalledTimes(tenantIds.length + 1)
    } finally {
      tenantIds.forEach((tenantId) => {
        jwksModule.deleteTenantJwksConfig(tenantId)
      })
      vi.doUnmock('@internal/cache')
      vi.resetModules()
      listActiveSpy.mockRestore()
    }
  })

  test('Config records one cache request per logical lookup', async () => {
    const listActiveSpy = vi.spyOn(jwksManager['storage'], 'listActive')
    const addSpy = vi.spyOn(cacheRequestsTotal, 'add')
    const lookupTenantId = 'jwks-cache-metrics-lookup'
    const encryptedJwk = {
      id: 'cache-metrics',
      kind: 'storage-url-signing-key',
      content: encrypt(JSON.stringify({ kty: 'oct', k: 'metric-cache-test-key' })),
    }

    const listActiveRequest = Promise.withResolvers<Array<typeof encryptedJwk>>()

    try {
      listActiveSpy.mockImplementation(() => listActiveRequest.promise)

      await assertLogicalLookupMetrics({
        addSpy,
        backendCallSpy: listActiveSpy,
        cacheName: TENANT_JWKS_CACHE_NAME,
        startLookups: () => [
          jwksManager.getJwksTenantConfig(lookupTenantId),
          jwksManager.getJwksTenantConfig(lookupTenantId),
          jwksManager.getJwksTenantConfig(lookupTenantId),
        ],
        resolveBackend: () => listActiveRequest.resolve([encryptedJwk]),
        assertCachedHit: async () => {
          await expect(jwksManager.getJwksTenantConfig(lookupTenantId)).resolves.toMatchObject({
            keys: [expect.objectContaining({ kid: 'storage-url-signing-key_cache-metrics' })],
          })
        },
      })
    } finally {
      listActiveSpy.mockRestore()
      addSpy.mockRestore()
    }
  })

  test('Generate all jwks status', async () => {
    const response = await adminApp.inject({
      method: 'GET',
      url: `/tenants/jwks/generate-all-missing`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(200)
    const statusBefore = response.json<{ running: boolean }>()
    expect(statusBefore.running).toBe(false)
  })

  test('Generate all jwks', async () => {
    const config = await jwksManager.getJwksTenantConfig(tenantId)
    expect(config.keys.length).toBe(1)
    const { kid } = config.keys[0]
    // disable url signing jwt added when tenant was created
    await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/${kid}`,
      payload: { active: false },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const queueInsertSpy = mockQueue().insertSpy
    const queueSpyAwaiter = new Promise((resolve) => {
      queueInsertSpy.mockImplementationOnce((...args) => resolve(args))
    })
    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/jwks/generate-all-missing`,
        payload: {},
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(200)
      const startData = response.json<{ started: boolean }>()
      expect(startData.started).toBe(true)

      await queueSpyAwaiter
      expect(queueInsertSpy).toHaveBeenCalledTimes(1)
      const [[callArg]] = queueInsertSpy.mock.calls
      expect(callArg).toHaveLength(1)
      expect(callArg[0]).toMatchObject({ data: { tenantId }, name: 'tenants-jwks-create-v2' })
    } finally {
      queueInsertSpy.mockRestore()
    }
  })

  test('Generate all jwks when already running', async () => {
    const statusSpy = vi
      .spyOn(UrlSigningJwkGenerator, 'getGenerationStatus')
      .mockReturnValueOnce({ running: true, sent: 99 })

    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/jwks/generate-all-missing`,
        payload: {},
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(400)
      expect(statusSpy).toHaveBeenCalledTimes(1)
    } finally {
      statusSpy.mockRestore()
    }
  })

  test('Ensure list tenants exits before yield if no items are returned', async () => {
    const listTenantsSpy = vi
      .spyOn(jwksManager['storage'], 'listTenantsWithoutKindPaginated')
      .mockResolvedValue([])
    try {
      const result = jwksManager.listTenantsMissingUrlSigningJwk(new AbortController().signal)

      let iterations = 0
      for await (const _ of result) {
        iterations++
      }
      expect(iterations).toBe(0)
      expect(listTenantsSpy).toHaveBeenCalledTimes(1)
    } finally {
      listTenantsSpy.mockRestore()
    }
  })

  test('Should use url signing jwk and fall back to old jwt secret when the jwk is removed', async () => {
    const configAwaiter = createJwkConfigChangeAwaiter()

    const secretWithJwk = await getJwtSecret(tenantId)
    expect(secretWithJwk.urlSigningKey).not.toBe(secretWithJwk.secret)
    expect(secretWithJwk.jwks.keys.length).toBe(1)
    expect(secretWithJwk.jwks.keys[0]).toEqual(secretWithJwk.urlSigningKey)

    const config = await jwksManager.getJwksTenantConfig(tenantId)
    expect(config.keys.length).toBe(1)
    const { kid } = config.keys[0]
    // disable url signing jwt added when tenant was created
    await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/${kid}`,
      payload: { active: false },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    await expect(configAwaiter).resolves.toBe(tenantId)

    deleteTenantConfig(tenantId)

    const secretWithoutJwk = await waitForEventually(
      () => getJwtSecret(tenantId),
      (value) => value.urlSigningKey === value.secret && value.jwks.keys.length === 0,
      `tenant ${tenantId} url signing fallback to jwtSecret`
    )
    expect(secretWithoutJwk.urlSigningKey).toBe(secretWithoutJwk.secret)
    expect(secretWithoutJwk.jwks.keys.length).toBe(0)
  })

  test('Ensure url signing jwk is idempotent', async () => {
    const config = await jwksManager.getJwksTenantConfig(tenantId)
    // signing jwk created automatically for new tenant
    expect(config.keys.length).toBe(1)
    const kidBefore = config.keys[0].kid

    const results = await Promise.all([
      jwksManager.generateUrlSigningJwk(tenantId),
      jwksManager.generateUrlSigningJwk(tenantId),
      jwksManager.generateUrlSigningJwk(tenantId),
    ])

    results.forEach((result) => expect(result.kid).toBe(kidBefore))
  })

  test('Storage.insert correctly throws if insert fails when not idempotent', async () => {
    const storage = new JWKSManagerStoreKnex(createMockKnexReturning([]))
    const insert = storage.insert('tenant-id', 'encrypted', 'kind')
    await expect(insert).rejects.toThrow('failed to insert jwk')
  })

  test('Storage.insert correctly throws if fails to find conflicting row during idempotent insert', async () => {
    const storage = new JWKSManagerStoreKnex(createMockKnexReturning({}))
    const insert = storage.insert('tenant-id', 'encrypted', 'kind', true)
    await expect(insert).rejects.toThrow('failed to find existing jwk on idempotent insert')
  })

  test('Roll url signing key', async () => {
    const queueSendSpy = mockQueue().sendSpy
    const queueSpyAwaiter = new Promise((resolve) => {
      queueSendSpy.mockImplementationOnce((...args) => {
        resolve(args)
      })
    })
    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/tenants/${tenantId}/jwks/url-signing/roll`,
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(200)
      const data = response.json<{ started: boolean }>()
      expect(data.started).toBe(true)

      await queueSpyAwaiter
      expect(queueSendSpy).toHaveBeenCalledTimes(1)
      const [[callArg]] = queueSendSpy.mock.calls
      expect(callArg).toMatchObject({
        data: { tenantId },
        name: 'tenants-jwks-roll-url-signing-key-v1',
      })
    } finally {
      queueSendSpy.mockRestore()
    }
  })

  test('Roll url signing key when no key exists', async () => {
    let configAwaiter = createJwkConfigChangeAwaiter()

    const config = await jwksManager.getJwksTenantConfig(tenantId)
    expect(config.keys.length).toBe(1)
    const { kid } = config.keys[0]

    await adminApp.inject({
      method: 'PUT',
      url: `/tenants/${tenantId}/jwks/${kid}`,
      payload: { active: false },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    await expect(configAwaiter).resolves.toBe(tenantId)

    const configBeforeRoll = await waitForEventually(
      () => jwksManager.getJwksTenantConfig(tenantId),
      (value) => value.keys.length === 0,
      `tenant ${tenantId} JWKS to clear after deactivation`
    )
    expect(configBeforeRoll.keys.length).toBe(0)

    configAwaiter = createJwkConfigChangeAwaiter()
    const { oldKid, newKid } = await jwksManager.rollUrlSigningJwk(tenantId)

    expect(oldKid).toBeNull()
    expect(newKid).toContain('storage-url-signing-key')

    await expect(configAwaiter).resolves.toBe(tenantId)
    const configAfterRoll = await waitForEventually(
      () => jwksManager.getJwksTenantConfig(tenantId),
      (value) => value.keys.length === 1,
      `tenant ${tenantId} JWKS to clear after deactivation`
    )
    expect(configAfterRoll.keys.length).toBe(1)
    expect(configAfterRoll.keys[0].kid).toBe(newKid)
  })

  test('Roll url signing key atomically replaces existing key', async () => {
    const configBefore = await jwksManager.getJwksTenantConfig(tenantId)
    expect(configBefore.keys.length).toBe(1)
    const oldKid = configBefore.keys[0].kid

    const configAwaiter = createJwkConfigChangeAwaiter()
    const { oldKid: returnedOldKid, newKid } = await jwksManager.rollUrlSigningJwk(tenantId)

    expect(returnedOldKid).toBe(oldKid)
    expect(newKid).not.toBe(oldKid)
    expect(newKid).toContain('storage-url-signing-key')

    await expect(configAwaiter).resolves.toBe(tenantId)
    const configAfter = await waitForEventually(
      () => jwksManager.getJwksTenantConfig(tenantId),
      (value) => value.keys[0].kid !== oldKid,
      `tenant ${tenantId} JWKS to clear after deactivation`
    )

    await jwksManager.getJwksTenantConfig(tenantId)
    expect(configAfter.keys.length).toBe(1)
    expect(configAfter.keys[0].kid).toBe(newKid)

    const activeKeys = await jwksManager['storage'].listActive(tenantId, 'storage-url-signing-key')
    expect(activeKeys.length).toBe(1)
    expect(activeKeys[0].id).toBe(newKid.split('_')[1])
  })
})
