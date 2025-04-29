import { getConfig, mergeConfig } from '../config'

getConfig()
mergeConfig({
  cdnPurgeEndpointURL: 'http://localhost/stub/cache',
  cdnPurgeEndpointKey: 'test-key',
})

import app from '../app'

jest.mock('axios', () => {
  const instance = {
    post: jest.fn(),
    interceptors: {
      request: {
        use: jest.fn(),
      },
      response: {
        use: jest.fn(),
      },
    },
  }

  return {
    create: jest.fn().mockReturnValue(instance),
    ...instance,
  }
})

import { useStorage } from './utils/storage'
import axios from 'axios'
import { Readable } from 'stream'
import { SignJWT } from 'jose'

const { serviceKeyAsync, anonKeyAsync, tenantId, jwtSecret } = getConfig()

describe('CDN Cache Manager', () => {
  const storageHook = useStorage()

  const bucketName = 'cdn-cache-manager-test-' + Date.now()
  beforeAll(async () => {
    await storageHook.storage.createBucket({
      id: bucketName,
      name: bucketName,
    })
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  afterAll(() => {
    getConfig({ reload: true })
  })

  it('will not allowing calling the purge endpoint without service_key', async () => {
    // cannot call with anon key
    const responseAnon = await app().inject({
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

    const responseAuthenticated = await app().inject({
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
      file: {
        body: Readable.from(Buffer.from('test')),
        cacheControl: 'public, max-age=31536000',
        mimeType: 'text/plain',
        isTruncated: () => false,
        userMetadata: {},
      },
    })

    const spy = jest
      .spyOn(axios, 'post')
      .mockReturnValue(Promise.resolve({ data: { message: 'success' } }))

    const response = await app().inject({
      method: 'DELETE',
      url: `/cdn/${bucketName}/${objectName}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })

    expect(response.statusCode).toBe(200)

    const body = await response.json()
    expect(body).toEqual({ message: 'success' })
    expect(spy).toBeCalledWith('/purge', {
      tenant: {
        ref: tenantId,
      },
      bucketId: bucketName,
      objectName: objectName,
    })
  })
})
