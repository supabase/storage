import type { Storage } from '@storage/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

type CdnConfig = {
  cdnPurgeEndpointURL?: string
  cdnPurgeEndpointKey?: string
}

async function importCdnCacheManager(config: CdnConfig = {}) {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig(config)

  return import('./cdn-cache-manager')
}

function createStorageMock(findObject = vi.fn().mockResolvedValue({ id: 'object-id' })) {
  const asSuperUser = vi.fn(() => ({ findObject }))
  const from = vi.fn(() => ({ asSuperUser }))

  return {
    storage: { from } as unknown as Storage,
    from,
    asSuperUser,
    findObject,
  }
}

describe('CdnCacheManager', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    vi.resetModules()
  })

  it('sends a purge request for an existing object', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)
    const timeoutSignal = new AbortController().signal
    const timeoutSpy = vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeoutSignal)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: 'test-key',
    })
    const storage = createStorageMock()

    await new CdnCacheManager(storage.storage).purge({
      tenant: 'tenant-ref',
      bucket: 'bucket-id',
      objectName: 'folder/image.png',
    })

    expect(storage.from).toHaveBeenCalledWith('bucket-id')
    expect(storage.asSuperUser).toHaveBeenCalledTimes(1)
    expect(storage.findObject).toHaveBeenCalledWith('folder/image.png')

    expect(fetchMock).toHaveBeenCalledTimes(1)

    const [input, init] = fetchMock.mock.calls[0]
    const requestInit = init as RequestInit & { dispatcher?: unknown }
    const headers = requestInit.headers as Headers

    expect(input.toString()).toBe('https://cdn.example.com/stub/cache/purge')
    expect(requestInit.method).toBe('POST')
    expect(headers.get('authorization')).toBe('Bearer test-key')
    expect(headers.get('content-type')).toBe('application/json')
    expect(JSON.parse(requestInit.body as string)).toEqual({
      tenant: {
        ref: 'tenant-ref',
      },
      bucketId: 'bucket-id',
      objectName: 'folder/image.png',
    })
    expect(requestInit.dispatcher).toBeDefined()
    expect(requestInit.signal).toBe(timeoutSignal)
    expect(timeoutSpy).toHaveBeenCalledWith(10_000)
  })

  it('omits the authorization header when the purge endpoint key is not configured', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: undefined,
    })
    const storage = createStorageMock()

    await new CdnCacheManager(storage.storage).purge({
      tenant: 'tenant-ref',
      bucket: 'bucket-id',
      objectName: 'folder/image.png',
    })

    const [, init] = fetchMock.mock.calls[0]
    const headers = (init as RequestInit).headers as Headers

    expect(headers.has('authorization')).toBe(false)
    expect(headers.get('content-type')).toBe('application/json')
  })

  it('wraps non-success purge responses and drains the response body', async () => {
    const cancel = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue({
      ok: false,
      status: 503,
      body: {
        cancel,
      },
    } as unknown as Response)
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: 'test-key',
    })
    const storage = createStorageMock()

    await expect(
      new CdnCacheManager(storage.storage).purge({
        tenant: 'tenant-ref',
        bucket: 'bucket-id',
        objectName: 'folder/image.png',
      })
    ).rejects.toMatchObject({
      code: 'InternalError',
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError: expect.objectContaining({
        message: 'Request failed with status code 503',
      }),
    })

    expect(cancel).toHaveBeenCalledTimes(1)
  })

  it('wraps network failures from fetch', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new Error('socket hang up'))
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: 'test-key',
    })
    const storage = createStorageMock()

    await expect(
      new CdnCacheManager(storage.storage).purge({
        tenant: 'tenant-ref',
        bucket: 'bucket-id',
        objectName: 'folder/image.png',
      })
    ).rejects.toMatchObject({
      code: 'InternalError',
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError: expect.objectContaining({
        message: 'socket hang up',
      }),
    })
  })

  it('wraps non-Error rejections from fetch', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue('socket hang up')
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: 'test-key',
    })
    const storage = createStorageMock()

    await expect(
      new CdnCacheManager(storage.storage).purge({
        tenant: 'tenant-ref',
        bucket: 'bucket-id',
        objectName: 'folder/image.png',
      })
    ).rejects.toMatchObject({
      code: 'InternalError',
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError: expect.objectContaining({
        message: 'socket hang up',
      }),
    })
  })

  it('does not call the purge endpoint when the object lookup fails', async () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointURL: 'https://cdn.example.com/stub/cache',
      cdnPurgeEndpointKey: 'test-key',
    })
    const lookupError = new Error('object not found')
    const storage = createStorageMock(vi.fn().mockRejectedValue(lookupError))

    await expect(
      new CdnCacheManager(storage.storage).purge({
        tenant: 'tenant-ref',
        bucket: 'bucket-id',
        objectName: 'missing.png',
      })
    ).rejects.toBe(lookupError)

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('requires a CDN purge endpoint URL before checking object existence', async () => {
    const fetchMock = vi.fn<typeof fetch>()
    vi.stubGlobal('fetch', fetchMock)

    const { CdnCacheManager } = await importCdnCacheManager({
      cdnPurgeEndpointKey: 'test-key',
    })
    const storage = createStorageMock()

    await expect(
      new CdnCacheManager(storage.storage).purge({
        tenant: 'tenant-ref',
        bucket: 'bucket-id',
        objectName: 'folder/image.png',
      })
    ).rejects.toMatchObject({
      code: 'MissingParameter',
      message: 'Missing Required Parameter CDN_PURGE_ENDPOINT_URL is not set',
    })

    expect(storage.from).not.toHaveBeenCalled()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})
