import { createHash } from 'node:crypto'
import { Readable } from 'node:stream'
import { vi } from 'vitest'

const {
  mockCreateStorage,
  mockDestroyConnection,
  mockGetKeyLocation,
  mockGetObject,
  mockFindObject,
  mockListObjectsForSignatureGeneration,
  mockLogError,
  mockLogEvent,
  mockLogWarning,
  mockUpdateObjectSignature,
} = vi.hoisted(() => ({
  mockCreateStorage: vi.fn(),
  mockDestroyConnection: vi.fn(),
  mockGetKeyLocation: vi.fn(),
  mockGetObject: vi.fn(),
  mockFindObject: vi.fn(),
  mockListObjectsForSignatureGeneration: vi.fn(),
  mockLogError: vi.fn(),
  mockLogEvent: vi.fn(),
  mockLogWarning: vi.fn(),
  mockUpdateObjectSignature: vi.fn(),
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {
    payload: unknown

    constructor(payload: unknown) {
      this.payload = payload
    }

    static send = vi.fn()
    static batchSend = vi.fn()

    static async createStorage(payload: unknown) {
      return mockCreateStorage(payload)
    }

    static getQueueName(this: { queueName: string }) {
      return this.queueName
    }
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    event: mockLogEvent,
    error: mockLogError,
    info: vi.fn(),
    warning: mockLogWarning,
  },
}))

vi.mock('../../../config', () => ({
  getConfig: () => ({
    storageS3Bucket: 'stub-storage-bucket',
  }),
}))

function makeStorage() {
  return {
    backend: {
      getObject: mockGetObject,
    },
    db: {
      destroyConnection: mockDestroyConnection,
      findObject: mockFindObject,
      listObjectsForSignatureGeneration: mockListObjectsForSignatureGeneration,
      updateObjectSignature: mockUpdateObjectSignature,
    },
    location: {
      getKeyLocation: mockGetKeyLocation,
    },
  }
}

function makeJob(data: Record<string, unknown>) {
  return {
    id: 'job-1',
    data: {
      tenant: { ref: 'tenant-a', host: '' },
      reqId: 'req-123',
      sbReqId: 'sb-req-123',
      ...data,
    },
  }
}

function makeObjectResponse(body: unknown) {
  return {
    body,
    httpStatusCode: 200,
    metadata: {
      cacheControl: 'no-cache',
      contentLength: Buffer.isBuffer(body) ? body.length : 0,
      eTag: 'etag',
      mimetype: 'text/plain',
      size: Buffer.isBuffer(body) ? body.length : 0,
    },
  }
}

describe('object signature generation events', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockCreateStorage.mockResolvedValue(makeStorage())
    mockDestroyConnection.mockResolvedValue(undefined)
    mockGetKeyLocation.mockImplementation(
      (locator: { tenantId: string; bucketId: string; objectName: string }) =>
        `${locator.tenantId}/${locator.bucketId}/${locator.objectName}`
    )
    mockFindObject.mockResolvedValue({ version: 'v1' })
  })

  it('fans out one signature generation job per listed object', async () => {
    const { GenerateObjectSignature, GenerateObjectSignatures } = await import(
      './generate-object-signature'
    )
    const generateBatchSend = vi
      .spyOn(GenerateObjectSignature, 'batchSend')
      .mockResolvedValue(undefined)

    mockListObjectsForSignatureGeneration.mockResolvedValue([
      { bucket_id: 'bucket-a', name: 'a.txt', version: 'v1' },
      { bucket_id: 'bucket-a', name: 'b.txt', version: 'v2' },
    ])

    await GenerateObjectSignatures.handle(
      makeJob({
        bucketId: 'bucket-a',
        force: false,
        batchSize: 10,
      }) as never
    )

    expect(mockListObjectsForSignatureGeneration).toHaveBeenCalledWith({
      bucketId: 'bucket-a',
      cursor: undefined,
      force: false,
      limit: 10,
      objectNames: undefined,
    })
    expect(generateBatchSend).toHaveBeenCalledTimes(1)
    expect(generateBatchSend.mock.calls[0][0].map((message) => message.payload)).toEqual([
      {
        tenant: { ref: 'tenant-a', host: '' },
        bucketId: 'bucket-a',
        objectName: 'a.txt',
        version: 'v1',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      },
      {
        tenant: { ref: 'tenant-a', host: '' },
        bucketId: 'bucket-a',
        objectName: 'b.txt',
        version: 'v2',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      },
    ])
    expect(mockDestroyConnection).toHaveBeenCalledTimes(1)
  })

  it('reschedules the coordinator when the batch is full', async () => {
    const { GenerateObjectSignature, GenerateObjectSignatures } = await import(
      './generate-object-signature'
    )
    vi.spyOn(GenerateObjectSignature, 'batchSend').mockResolvedValue(undefined)
    const generateSend = vi.spyOn(GenerateObjectSignatures, 'send').mockResolvedValue(undefined)

    mockListObjectsForSignatureGeneration.mockResolvedValue([
      { bucket_id: 'bucket-a', name: 'a.txt', version: 'v1' },
      { bucket_id: 'bucket-b', name: 'z.txt', version: 'v2' },
    ])

    await GenerateObjectSignatures.handle(
      makeJob({
        force: true,
        batchSize: 2,
      }) as never
    )

    expect(generateSend).toHaveBeenCalledWith({
      tenant: { ref: 'tenant-a', host: '' },
      cursor: { bucketId: 'bucket-b', objectName: 'z.txt' },
      force: true,
      reqId: 'req-123',
      sbReqId: 'sb-req-123',
      batchSize: 2,
    })
  })

  it('logs coordinator throughput after processing a batch', async () => {
    const { GenerateObjectSignature, GenerateObjectSignatures } = await import(
      './generate-object-signature'
    )
    vi.spyOn(GenerateObjectSignature, 'batchSend').mockResolvedValue(undefined)
    vi.spyOn(GenerateObjectSignatures, 'send').mockResolvedValue(undefined)

    mockListObjectsForSignatureGeneration.mockResolvedValue([
      { bucket_id: 'bucket-a', name: 'a.txt', version: 'v1' },
      { bucket_id: 'bucket-b', name: 'z.txt', version: 'v2' },
    ])

    await GenerateObjectSignatures.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectNames: ['a.txt', 'z.txt'],
        force: true,
        batchSize: 2,
      }) as never
    )

    expect(mockLogEvent).toHaveBeenCalledWith(
      expect.anything(),
      '[Admin]: GenerateObjectSignatures tenant-a processed 2 objects',
      expect.objectContaining({
        event: 'GenerateObjectSignatures',
        jobId: 'job-1',
        objectPath: 'tenant-a',
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      })
    )

    const [, , log] = mockLogEvent.mock.calls[0]
    expect(JSON.parse(log.metadata)).toEqual({
      batchSize: 2,
      bucketId: 'bucket-a',
      cursor: null,
      force: true,
      objectNamesCount: 2,
      objectsCount: 2,
      rescheduled: true,
    })
  })

  it.each([
    { batchSize: Number.NaN, expectedLimit: 500 },
    { batchSize: 0, expectedLimit: 500 },
    { batchSize: -10, expectedLimit: 500 },
    { batchSize: 1200, expectedLimit: 1000 },
  ])('normalizes coordinator batch size $batchSize to $expectedLimit in query and reschedule payload', async (testCase) => {
    const { GenerateObjectSignature, GenerateObjectSignatures } = await import(
      './generate-object-signature'
    )
    vi.spyOn(GenerateObjectSignature, 'batchSend').mockResolvedValue(undefined)
    const generateSend = vi.spyOn(GenerateObjectSignatures, 'send').mockResolvedValue(undefined)
    mockListObjectsForSignatureGeneration.mockResolvedValue(
      Array.from({ length: testCase.expectedLimit }, (_, index) => ({
        bucket_id: 'bucket-a',
        name: `object-${index}`,
        version: `v${index}`,
      }))
    )

    await GenerateObjectSignatures.handle(
      makeJob({
        batchSize: testCase.batchSize,
      }) as never
    )

    expect(mockListObjectsForSignatureGeneration).toHaveBeenCalledWith(
      expect.objectContaining({
        limit: testCase.expectedLimit,
      })
    )
    expect(generateSend).toHaveBeenCalledWith(
      expect.objectContaining({
        batchSize: testCase.expectedLimit,
        cursor: { bucketId: 'bucket-a', objectName: `object-${testCase.expectedLimit - 1}` },
      })
    )
  })

  it('serializes coordinator fanout per tenant', async () => {
    const { GenerateObjectSignatures } = await import('./generate-object-signature')

    expect(GenerateObjectSignatures.getQueueOptions()).toEqual({
      name: 'object:signatures:generate',
      policy: 'singleton',
    })
    expect(
      GenerateObjectSignatures.getSendOptions({
        tenant: { ref: 'tenant-a', host: '' },
        bucketId: 'bucket-a',
        objectNames: ['a.txt'],
      })
    ).toEqual(
      expect.objectContaining({
        singletonKey: 'tenant-a',
        priority: 5,
        retryLimit: 5,
        retryDelay: 5,
      })
    )
  })

  it('limits per-object signature hashing worker concurrency', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    expect(GenerateObjectSignature.getWorkerOptions()).toEqual(
      expect.objectContaining({
        concurrentTaskCount: 8,
      })
    )
  })

  it('deduplicates queued per-object signature jobs by singleton key', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    expect(GenerateObjectSignature.getQueueOptions()).toEqual({
      name: 'object:signature:generate',
      policy: 'exactly_once',
    })
  })

  it('hashes backend bytes and updates the matching object version', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const payload = Buffer.from('hello world')
    const expectedHex = createHash('sha256').update(payload).digest('hex')

    mockGetObject.mockResolvedValue({
      body: Readable.from([payload.subarray(0, 5), payload.subarray(5)]),
      httpStatusCode: 200,
      metadata: {
        cacheControl: 'no-cache',
        contentLength: payload.length,
        eTag: 'etag',
        mimetype: 'text/plain',
        size: payload.length,
      },
    })
    mockUpdateObjectSignature.mockResolvedValue(true)

    await GenerateObjectSignature.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectName: 'a.txt',
        version: 'v1',
      }) as never
    )

    expect(mockGetObject).toHaveBeenCalledWith(
      'stub-storage-bucket',
      'tenant-a/bucket-a/a.txt',
      'v1',
      undefined,
      undefined
    )
    expect(mockUpdateObjectSignature).toHaveBeenCalledWith(
      'bucket-a',
      'a.txt',
      'v1',
      Buffer.from(expectedHex, 'hex')
    )
    expect(mockLogEvent).toHaveBeenCalledWith(
      expect.anything(),
      '[Admin]: GenerateObjectSignature tenant-a/bucket-a/a.txt',
      expect.objectContaining({
        event: 'GenerateObjectSignature',
        objectPath: 'tenant-a/bucket-a/a.txt',
        tenantId: 'tenant-a',
        project: 'tenant-a',
        metadata: JSON.stringify({ version: 'v1' }),
      })
    )
    expect(mockDestroyConnection).toHaveBeenCalledTimes(1)
  })

  it('passes the queue abort signal to the backend object read', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const abortController = new AbortController()

    mockGetObject.mockResolvedValue(makeObjectResponse(Buffer.from('abortable object')))
    mockUpdateObjectSignature.mockResolvedValue(true)

    await GenerateObjectSignature.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectName: 'a.txt',
        version: 'v1',
      }) as never,
      { signal: abortController.signal }
    )

    expect(mockGetObject).toHaveBeenCalledWith(
      'stub-storage-bucket',
      'tenant-a/bucket-a/a.txt',
      'v1',
      undefined,
      abortController.signal
    )
  })

  it('stops streaming signature hashing when the queue abort signal fires', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const abortController = new AbortController()
    const abortError = new DOMException('worker stopped', 'AbortError')
    const body = Readable.from(
      (async function* () {
        yield Buffer.from('first chunk')
        abortController.abort(abortError)
        yield Buffer.from('second chunk')
      })()
    )

    mockGetObject.mockResolvedValue(makeObjectResponse(body))

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never,
        { signal: abortController.signal }
      )
    ).rejects.toBe(abortError)

    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
  })

  it('hashes and updates objects without a version', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const payload = Buffer.from('versionless object')
    const expectedHex = createHash('sha256').update(payload).digest('hex')

    mockFindObject.mockResolvedValue({ version: null })
    mockGetObject.mockResolvedValue(makeObjectResponse(payload))
    mockUpdateObjectSignature.mockResolvedValue(true)

    await GenerateObjectSignature.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectName: 'a.txt',
      }) as never
    )

    expect(mockGetObject).toHaveBeenCalledWith(
      'stub-storage-bucket',
      'tenant-a/bucket-a/a.txt',
      undefined,
      undefined,
      undefined
    )
    expect(mockUpdateObjectSignature).toHaveBeenCalledWith(
      'bucket-a',
      'a.txt',
      undefined,
      Buffer.from(expectedHex, 'hex')
    )
  })

  it.each([
    { label: 'updated', currentObject: { version: 'v2' } },
    { label: 'deleted', currentObject: undefined },
  ])('skips $label stale object rows before reading backend bytes', async (testCase) => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockFindObject.mockResolvedValue(testCase.currentObject)

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).resolves.toBeUndefined()

    expect(mockFindObject).toHaveBeenCalledWith('bucket-a', 'a.txt', 'version', {
      dontErrorOnEmpty: true,
    })
    expect(mockGetObject).not.toHaveBeenCalled()
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
    expect(mockLogEvent).not.toHaveBeenCalled()
    expect(mockLogError).not.toHaveBeenCalled()
    const [, message, log] = mockLogWarning.mock.calls[0]
    expect(message).toBe(
      '[Admin]: GenerateObjectSignature tenant-a/bucket-a/a.txt skipped stale object'
    )
    expect(log).toEqual(
      expect.objectContaining({
        event: 'GenerateObjectSignature',
        objectPath: 'tenant-a/bucket-a/a.txt',
      })
    )
    expect(JSON.parse(log.metadata)).toEqual({
      bucketId: 'bucket-a',
      objectName: 'a.txt',
      objectPath: 'tenant-a/bucket-a/a.txt',
      version: 'v1',
    })
  })

  it('treats backend read failures as stale when the row changes after the pre-read check', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const error = new Error('backend object version was removed')

    mockFindObject.mockResolvedValueOnce({ version: 'v1' }).mockResolvedValueOnce({ version: 'v2' })
    mockGetObject.mockRejectedValue(error)

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).resolves.toBeUndefined()

    expect(mockFindObject).toHaveBeenNthCalledWith(1, 'bucket-a', 'a.txt', 'version', {
      dontErrorOnEmpty: true,
    })
    expect(mockFindObject).toHaveBeenNthCalledWith(2, 'bucket-a', 'a.txt', 'version', {
      dontErrorOnEmpty: true,
    })
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
    expect(mockLogError).not.toHaveBeenCalled()
    const [, message] = mockLogWarning.mock.calls[0]
    expect(message).toBe(
      '[Admin]: GenerateObjectSignature tenant-a/bucket-a/a.txt skipped stale object'
    )
  })

  it('skips stale object rows when no object row is updated after hashing', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockGetObject.mockResolvedValue(makeObjectResponse(Buffer.from('stale object')))
    mockUpdateObjectSignature.mockResolvedValue(false)

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).resolves.toBeUndefined()

    expect(mockLogEvent).not.toHaveBeenCalled()
    expect(mockLogError).not.toHaveBeenCalled()
    const [, message, log] = mockLogWarning.mock.calls[0]
    expect(message).toBe(
      '[Admin]: GenerateObjectSignature tenant-a/bucket-a/a.txt skipped stale object'
    )
    expect(log).toEqual(
      expect.objectContaining({
        event: 'GenerateObjectSignature',
        objectPath: 'tenant-a/bucket-a/a.txt',
      })
    )
    expect(JSON.parse(log.metadata)).toEqual({
      bucketId: 'bucket-a',
      objectName: 'a.txt',
      objectPath: 'tenant-a/bucket-a/a.txt',
      version: 'v1',
    })
  })

  it.each([
    {
      label: 'Buffer',
      body: Buffer.from('buffer body'),
      expectedPayload: Buffer.from('buffer body'),
    },
    {
      label: 'Blob',
      body: new Blob([Buffer.from('blob body')]),
      expectedPayload: Buffer.from('blob body'),
    },
    {
      label: 'web ReadableStream',
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(Buffer.from('web stream body'))
          controller.close()
        },
      }),
      expectedPayload: Buffer.from('web stream body'),
    },
  ])('hashes $label object bodies', async (testCase) => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const expectedHex = createHash('sha256').update(testCase.expectedPayload).digest('hex')

    mockGetObject.mockResolvedValue(makeObjectResponse(testCase.body))
    mockUpdateObjectSignature.mockResolvedValue(true)

    await GenerateObjectSignature.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectName: 'a.txt',
        version: 'v1',
      }) as never
    )

    expect(mockUpdateObjectSignature).toHaveBeenCalledWith(
      'bucket-a',
      'a.txt',
      'v1',
      Buffer.from(expectedHex, 'hex')
    )
  })

  it('streams Blob object bodies instead of materializing them with arrayBuffer', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const body = new Blob([Buffer.from('streamed blob body')])
    const expectedHex = createHash('sha256').update(Buffer.from('streamed blob body')).digest('hex')
    const arrayBufferSpy = vi
      .spyOn(Blob.prototype, 'arrayBuffer')
      .mockRejectedValue(new Error('arrayBuffer should not be called'))

    try {
      mockGetObject.mockResolvedValue(makeObjectResponse(body))
      mockUpdateObjectSignature.mockResolvedValue(true)

      await GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    } finally {
      arrayBufferSpy.mockRestore()
    }

    expect(mockUpdateObjectSignature).toHaveBeenCalledWith(
      'bucket-a',
      'a.txt',
      'v1',
      Buffer.from(expectedHex, 'hex')
    )
  })

  it('does not stringify undefined versions in the singleton key', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const singletonKey = createHash('sha256')
      .update(JSON.stringify(['tenant-a', 'bucket-a', 'a.txt', null]))
      .digest('hex')

    expect(
      GenerateObjectSignature.getSendOptions({
        tenant: { ref: 'tenant-a', host: '' },
        bucketId: 'bucket-a',
        objectName: 'a.txt',
      })
    ).toEqual(
      expect.objectContaining({
        expireInMinutes: 120,
        singletonKey,
      })
    )
  })

  it('uses tuple encoding for singleton keys before hashing', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const tupleEncodedKey = createHash('sha256')
      .update(JSON.stringify(['tenant-a', 'bucket-a', 'a/b.txt', 'v1']))
      .digest('hex')

    expect(
      GenerateObjectSignature.getSendOptions({
        tenant: { ref: 'tenant-a', host: '' },
        bucketId: 'bucket-a',
        objectName: 'a/b.txt',
        version: 'v1',
      })
    ).toEqual(expect.objectContaining({ singletonKey: tupleEncodedKey }))
  })

  it('rejects string chunks instead of hashing UTF-8 encoded text', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockGetObject.mockResolvedValue({
      body: Readable.from(['hello world']),
      httpStatusCode: 200,
      metadata: {
        cacheControl: 'no-cache',
        contentLength: 11,
        eTag: 'etag',
        mimetype: 'text/plain',
        size: 11,
      },
    })

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).rejects.toThrow('Unsupported object body string chunk for SHA-256 hashing')
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
  })

  it('rejects missing object bodies', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockGetObject.mockResolvedValue(makeObjectResponse(undefined))

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).rejects.toThrow('Object body is missing for SHA-256 hashing')
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
  })

  it('rejects unsupported object body types', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockGetObject.mockResolvedValue(makeObjectResponse(42))

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).rejects.toThrow('Unsupported object body type for SHA-256 hashing')
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
  })

  it('rejects objects with non-callable getReader as unsupported body types', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')

    mockGetObject.mockResolvedValue(makeObjectResponse({ getReader: 'not-a-function' }))

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).rejects.toThrow('Unsupported object body type for SHA-256 hashing')
    expect(mockUpdateObjectSignature).not.toHaveBeenCalled()
  })

  it('logs scoped details when the coordinator fails', async () => {
    const { GenerateObjectSignatures } = await import('./generate-object-signature')
    const error = new Error('list failed')

    mockListObjectsForSignatureGeneration.mockRejectedValue(error)

    await expect(
      GenerateObjectSignatures.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectNames: ['a.txt', 'b.txt'],
          force: true,
          batchSize: 10,
          cursor: { bucketId: 'bucket-a', objectName: 'a.txt' },
        }) as never
      )
    ).rejects.toThrow(error)

    const [, message, log] = mockLogError.mock.calls[0]
    expect(message).toBe('[Admin]: GenerateObjectSignatures tenant-a - FAILED')
    expect(log).toEqual(
      expect.objectContaining({
        error,
        type: 'event',
        event: 'GenerateObjectSignatures',
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      })
    )
    expect(JSON.parse(log.metadata)).toEqual({
      batchSize: 10,
      bucketId: 'bucket-a',
      cursor: { bucketId: 'bucket-a', objectName: 'a.txt' },
      force: true,
      objectNamesCount: 2,
    })
  })

  it('logs object details when a signature job fails', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const error = new Error('read failed')

    mockGetObject.mockRejectedValue(error)

    await expect(
      GenerateObjectSignature.handle(
        makeJob({
          bucketId: 'bucket-a',
          objectName: 'a.txt',
          version: 'v1',
        }) as never
      )
    ).rejects.toThrow(error)

    const [, message, log] = mockLogError.mock.calls[0]
    expect(message).toBe('[Admin]: GenerateObjectSignature tenant-a/bucket-a/a.txt - FAILED')
    expect(log).toEqual(
      expect.objectContaining({
        error,
        type: 'event',
        event: 'GenerateObjectSignature',
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      })
    )
    expect(JSON.parse(log.metadata)).toEqual({
      bucketId: 'bucket-a',
      objectName: 'a.txt',
      objectPath: 'tenant-a/bucket-a/a.txt',
      version: 'v1',
    })
  })

  it('logs only tenant context when connection disposal fails', async () => {
    const { GenerateObjectSignature } = await import('./generate-object-signature')
    const error = new Error('disconnect failed')

    mockGetObject.mockResolvedValue({
      body: Buffer.from('hello world'),
      httpStatusCode: 200,
      metadata: {
        cacheControl: 'no-cache',
        contentLength: 11,
        eTag: 'etag',
        mimetype: 'text/plain',
        size: 11,
      },
    })
    mockUpdateObjectSignature.mockResolvedValue(true)
    mockDestroyConnection.mockRejectedValue(error)

    await GenerateObjectSignature.handle(
      makeJob({
        bucketId: 'bucket-a',
        objectName: 'a.txt',
        version: 'v1',
      }) as never
    )
    await new Promise(process.nextTick)

    const [, message, log] = mockLogError.mock.calls[0]
    expect(message).toBe('[Admin]: GenerateObjectSignature tenant-a - FAILED DISPOSING CONNECTION')
    expect(log).toEqual(
      expect.objectContaining({
        error,
        type: 'event',
        tenantId: 'tenant-a',
        project: 'tenant-a',
        reqId: 'req-123',
        sbReqId: 'sb-req-123',
      })
    )
    expect(log).not.toHaveProperty('metadata')
  })
})
