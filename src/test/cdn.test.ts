import { CdnCacheManager } from '@storage/cdn/cdn-cache-manager'
import { FastifyInstance } from 'fastify'
import { SignJWT } from 'jose'
import { Readable } from 'stream'
import { getConfig, mergeConfig } from '../config'
import { useStorage } from './utils/storage'

getConfig()
mergeConfig({
  cdnPurgeEndpointURL: 'http://localhost/stub/cache',
  cdnPurgeEndpointKey: 'test-key',
})

const { serviceKeyAsync, anonKeyAsync, tenantId, jwtSecret } = getConfig()

describe('CDN Cache Manager', () => {
  const storageHook = useStorage()
  let appInstance: FastifyInstance
  let buildApp: typeof import('../app').default

  const bucketName = 'cdn-cache-manager-test-' + Date.now()
  beforeAll(async () => {
    await storageHook.storage.createBucket({
      id: bucketName,
      name: bucketName,
    })
    buildApp = (await import('../app')).default
  })

  beforeEach(() => {
    appInstance = buildApp()
  })

  afterEach(async () => {
    await appInstance.close()
    vi.restoreAllMocks()
    vi.clearAllMocks()
  })

  afterAll(() => {
    getConfig({ reload: true })
  })

  it('will not allowing calling the purge endpoint without service_key', async () => {
    // cannot call with anon key
    const responseAnon = await appInstance.inject({
      method: 'DELETE',
      url: `/cdn/${bucketName}/test-anon.txt§`,
      headers: {
        authorization: `Bearer ${await anonKeyAsync}`,
      },
    })

    expect(responseAnon.statusCode).toBe(403)

    // Cannot call with authenticated token
    const authenticatedJwt = await new SignJWT({ role: 'authenticated', sub: 'user-id' })
      .setIssuedAt()
      .setProtectedHeader({ alg: 'HS256' })
      .sign(new TextEncoder().encode(jwtSecret))

    const responseAuthenticated = await appInstance.inject({
      method: 'DELETE',
      url: `/cdn/${bucketName}/test-anon.txt§`,
      headers: {
        authorization: `Bearer ${authenticatedJwt}`,
      },
    })

    expect(responseAuthenticated.statusCode).toBe(403)
  })

  it('will allow calling the purge endpoint when using service_key', async () => {
    const objectName = `purge-file-${Date.now()}.txt`
    await storageHook.storage.from(bucketName).uploadNewObject({
      isUpsert: true,
      objectName,
      userMetadata: {},
      file: {
        body: Readable.from(Buffer.from('test')),
        cacheControl: 'public, max-age=31536000',
        mimeType: 'text/plain',
        isTruncated: () => false,
      },
    })

    const purgeSpy = vi.spyOn(CdnCacheManager.prototype, 'purge').mockResolvedValue(undefined)

    const response = await appInstance.inject({
      method: 'DELETE',
      url: `/cdn/${bucketName}/${objectName}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })

    expect(response.statusCode).toBe(200)

    const body = await response.json()
    expect(body).toEqual({ message: 'success' })
    expect(purgeSpy).toHaveBeenCalledWith({
      tenant: tenantId,
      bucket: bucketName,
      objectName,
    })
  })
})
