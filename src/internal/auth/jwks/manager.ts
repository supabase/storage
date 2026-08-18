import { decrypt, encrypt, generateHS512JWK } from '@internal/auth'
import {
  CACHE_LOOKUP_WITHOUT_METRICS,
  type CacheLookupOptions,
  createLruCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  DEFAULT_CACHE_TTL_JITTER_RATIO,
  TENANT_JWKS_CACHE_NAME,
} from '@internal/cache'
import { createInvalidatableSingleFlightByKey } from '@internal/concurrency'
import { isStringMessage, PubSubAdapter } from '@internal/pubsub'
import { freezeJwksConfig, JwksConfig, JwksConfigKeyOCT } from '../../../config'
import { JWK_KIND_STORAGE_URL_SIGNING, TENANTS_JWKS_UPDATE_CHANNEL } from './constants'
import { JWK_KID_SEPARATOR } from './kid'
import { JWKSManagerStore } from './store'

const tenantJwksSingleFlight = createInvalidatableSingleFlightByKey<JwksConfig>()
// Max 16,384 items. At ~2.5KB per JWKS, this uses roughly ~40MB of heap memory worst-case.
export const TENANT_JWKS_CACHE_MAX_ITEMS = 16384
export const TENANT_JWKS_CACHE_TTL_MS = 1000 * 60 * 60 // 1h

const tenantJwksConfigCache = createLruCache<string, JwksConfig>(TENANT_JWKS_CACHE_NAME, {
  max: TENANT_JWKS_CACHE_MAX_ITEMS,
  ttl: TENANT_JWKS_CACHE_TTL_MS,
  ttlJitterRatio: DEFAULT_CACHE_TTL_JITTER_RATIO,
  allowStale: false,
  purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
})

export function deleteTenantJwksConfig(tenantId: string): void {
  tenantJwksSingleFlight.invalidate(tenantId)
  tenantJwksConfigCache.delete(tenantId)
}

function createJwkKid({ kind, id }: { id: string; kind: string }): string {
  return kind + JWK_KID_SEPARATOR + id
}

function getJwkIdFromKid(kid: string): string {
  return kid.split(JWK_KID_SEPARATOR).pop() as string
}

export class JWKSManager<TRX> {
  constructor(private storage: JWKSManagerStore<TRX>) {}

  /**
   * Keeps the in memory config cache up to date
   */
  async listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
    await pubSub.subscribe(TENANTS_JWKS_UPDATE_CHANNEL, (cacheKey) => {
      if (!isStringMessage(cacheKey)) {
        return
      }

      deleteTenantJwksConfig(cacheKey)
    })
  }

  /**
   * Generates a new URL signing JWK and stores it in the database if one does not already exist.
   * Only one active url signing jwk can exist, this function is idempotent and will create a new entry or return the kid of the existing
   * @param tenantId
   * @param trx optional transaction to add the jwk within
   */
  async generateUrlSigningJwk(tenantId: string, trx?: TRX): Promise<{ kid: string }> {
    const content = encrypt(JSON.stringify(await generateHS512JWK()))
    const id = await this.storage.insert(tenantId, content, JWK_KIND_STORAGE_URL_SIGNING, true, trx)
    return { kid: createJwkKid({ kind: JWK_KIND_STORAGE_URL_SIGNING, id }) }
  }

  /**
   * Atomically rolls the URL signing JWK by deactivating the current key and creating a new one
   * @param tenantId
   */
  async rollUrlSigningJwk(tenantId: string): Promise<{ oldKid: string | null; newKid: string }> {
    return this.storage.transaction(async (trx) => {
      const currentKeys = await this.storage.listActive(tenantId, JWK_KIND_STORAGE_URL_SIGNING, trx)
      const currentKey = currentKeys[0]

      if (currentKey) {
        await this.storage.toggleActive(tenantId, currentKey.id, false, trx)
      }

      const { kid: newKid } = await this.generateUrlSigningJwk(tenantId, trx)

      return {
        oldKid: currentKey
          ? createJwkKid({ kind: JWK_KIND_STORAGE_URL_SIGNING, id: currentKey.id })
          : null,
        newKid,
      }
    })
  }

  /**
   * Adds a new jwk that can be used for signing urls
   * @param tenantId
   * @param jwk jwk content
   * @param kind string used to identify the purpose or source of each jwk
   */
  async addJwk(tenantId: string, jwk: object, kind: string): Promise<{ kid: string }> {
    const id = await this.storage.insert(tenantId, encrypt(JSON.stringify(jwk)), kind)
    return { kid: createJwkKid({ kind, id }) }
  }

  /**
   * Disables an existing jwk, is no longer valid for signed urls
   * @param tenantId
   * @param kid
   */
  toggleJwkActive(tenantId: string, kid: string, newState: boolean): Promise<boolean> {
    return this.storage.toggleActive(tenantId, getJwkIdFromKid(kid), newState)
  }

  /**
   * Queries the tenant jwks from the multi-tenant database and stores them in a local cache
   * for quick subsequent access. Only includes jwks marked as active
   * @param tenantId
   */
  async getJwksTenantConfig(tenantId: string, options?: CacheLookupOptions): Promise<JwksConfig> {
    const cachedJwks = tenantJwksConfigCache.get(tenantId, options)

    if (cachedJwks !== undefined) {
      return cachedJwks
    }

    return tenantJwksSingleFlight(tenantId, {
      load: async () => {
        const data = await this.storage.listActive(tenantId)

        let urlSigningKey: JwksConfigKeyOCT | undefined
        const keys = data.map(({ id, kind, content }) => {
          const jwk = JSON.parse(decrypt(content))
          jwk.kid = createJwkKid({ kind, id })
          const isUrlSigningKeyKind = kind === JWK_KIND_STORAGE_URL_SIGNING
          if (isUrlSigningKeyKind && jwk.kty === 'oct' && jwk.k && !urlSigningKey) {
            urlSigningKey = jwk
          }
          return jwk
        })
        const jwksConfig = freezeJwksConfig({ keys, urlSigningKey })

        return jwksConfig
      },
      retry: () => this.getJwksTenantConfig(tenantId, CACHE_LOOKUP_WITHOUT_METRICS),
      commit: (jwksConfig) => tenantJwksConfigCache.set(tenantId, jwksConfig),
    })
  }

  /**
   * Gets a list of all tenants that do not have a signing url associated
   */
  async *listTenantsMissingUrlSigningJwk(
    signal: AbortSignal,
    batchSize = 200
  ): AsyncGenerator<string[]> {
    let lastCursor = 0

    while (!signal.aborted) {
      const data = await this.storage.listTenantsWithoutKindPaginated(
        JWK_KIND_STORAGE_URL_SIGNING,
        batchSize,
        lastCursor
      )
      if (data.length === 0) {
        break
      }

      lastCursor = data[data.length - 1].cursor_id
      yield data.map((tenant) => tenant.id)
    }
  }
}
