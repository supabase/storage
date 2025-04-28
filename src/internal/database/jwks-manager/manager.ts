import { createMutexByKey } from '@internal/concurrency'
import { JwksConfig, JwksConfigKeyOCT } from '../../../config'
import { JWKSManagerStore } from './store'
import { PubSubAdapter } from '@internal/pubsub'
import { decrypt, encrypt, generateHS256JWK } from '@internal/auth'
import { Knex } from 'knex'

const TENANTS_JWKS_UPDATE_CHANNEL = 'tenants_jwks_update'
const JWK_KIND_STORAGE_URL_SIGNING = 'storage-url-signing-key'
const JWK_KID_SEPARATOR = '_'

const tenantJwksMutex = createMutexByKey<JwksConfig>()
const tenantJwksConfigCache = new Map<string, JwksConfig>()

function createJwkKid({ kind, id }: { id: string; kind: string }): string {
  return kind + JWK_KID_SEPARATOR + id
}

function getJwkIdFromKid(kid: string): string {
  return kid.split(JWK_KID_SEPARATOR).pop() as string
}

export class JWKSManager {
  constructor(private storage: JWKSManagerStore<Knex.Transaction>) {}

  /**
   * Keeps the in memory config cache up to date
   */
  async listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
    await pubSub.subscribe(TENANTS_JWKS_UPDATE_CHANNEL, (cacheKey) => {
      tenantJwksConfigCache.delete(cacheKey)
    })
  }

  /**
   * Generates a new URL signing JWK and stores it in the database if one does not already exist.
   * Only one active url signing jwk can exist, this function is idempotent and will create a new entry or return the kid of the existing
   * @param tenantId
   * @param trx optional transaction to add the jwk within
   */
  async generateUrlSigningJwk(tenantId: string, trx?: Knex.Transaction): Promise<{ kid: string }> {
    const content = encrypt(JSON.stringify(generateHS256JWK()))
    const id = await this.storage.insert(tenantId, content, JWK_KIND_STORAGE_URL_SIGNING, true, trx)
    return { kid: createJwkKid({ kind: JWK_KIND_STORAGE_URL_SIGNING, id }) }
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
  async getJwksTenantConfig(tenantId: string): Promise<JwksConfig> {
    const cachedJwks = tenantJwksConfigCache.get(tenantId)

    if (cachedJwks) {
      return cachedJwks
    }

    return tenantJwksMutex(tenantId, async () => {
      const cachedJwks = tenantJwksConfigCache.get(tenantId)

      if (cachedJwks) {
        return cachedJwks
      }

      const data = await this.storage.listActive(tenantId)

      let urlSigningKey: JwksConfigKeyOCT | undefined
      const jwksConfig: JwksConfig = {
        keys: data.map(({ id, kind, content }) => {
          const jwk = JSON.parse(decrypt(content))
          jwk.kid = createJwkKid({ kind, id })
          if (
            kind === JWK_KIND_STORAGE_URL_SIGNING &&
            jwk.kty === 'oct' &&
            jwk.k &&
            !urlSigningKey
          ) {
            urlSigningKey = jwk
          }
          return jwk
        }),
      }
      jwksConfig.urlSigningKey = urlSigningKey

      tenantJwksConfigCache.set(tenantId, jwksConfig)

      return jwksConfig
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
