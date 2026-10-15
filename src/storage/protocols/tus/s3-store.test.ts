import fs from 'node:fs'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { Readable } from 'node:stream'
import { HttpResponse } from '@smithy/protocol-http'
import { Upload } from '@tus/server'
import { S3Store } from './s3-store'

class TestS3Store extends S3Store {
  private readonly uploadMetadata = {
    file: new Upload({ id: 'upload-id', size: 32 * 1024 * 1024, offset: 0 }),
    'upload-id': 'multipart-id',
    'tus-version': '1.0.0',
  }

  getClient() {
    return this.client
  }

  uploadPartsForTest(body: Buffer) {
    return this.uploadParts(this.uploadMetadata, Readable.from([body]), 1, 0)
  }

  uploadBufferForTest(body: Buffer) {
    return this.uploadPart(this.uploadMetadata, body, 1)
  }
}

function createStore(
  handle: (request: unknown, options?: unknown) => Promise<{ response: HttpResponse }>,
  maxAttempts = 1
) {
  return new TestS3Store({
    s3ClientConfig: {
      bucket: 'test-bucket',
      region: 'us-east-1',
      endpoint: 'http://127.0.0.1:9000',
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
      maxAttempts,
      requestHandler: { handle },
    },
  })
}

describe('S3Store', () => {
  test('create matches upstream apart from ContentEncoding', async () => {
    const store = createStore(vi.fn())
    const create = vi.spyOn(store.getClient(), 'createMultipartUpload').mockResolvedValue({
      Key: 'upload-id',
      UploadId: 'multipart-id',
      $metadata: {},
    } as never)
    const put = vi
      .spyOn(store.getClient(), 'putObject')
      .mockResolvedValue({ $metadata: {} } as never)
    const metadata = { contentType: 'text/plain', cacheControl: 'no-transform' }

    const plain = await store.create(new Upload({ id: 'upload-id', size: 10, offset: 0, metadata }))
    const encoded = await store.create(
      new Upload({
        id: 'upload-id',
        size: 10,
        offset: 0,
        metadata: { ...metadata, contentEncoding: 'gzip' },
      })
    )

    expect(create.mock.calls[1][0]).toEqual({ ...create.mock.calls[0][0], ContentEncoding: 'gzip' })
    expect(put.mock.calls[1][0].Metadata).toEqual(put.mock.calls[0][0].Metadata)
    expect(encoded.storage).toEqual(plain.storage)
  })

  test('removes the no-op logger middleware from the internal TUS client', () => {
    const store = createStore(vi.fn())

    expect(
      store
        .getClient()
        .middlewareStack.identify()
        .some((middleware) => middleware.includes('loggerMiddleware'))
    ).toBe(false)
  })

  test('preserves SDK retries after removing the logger middleware', async () => {
    const handle = vi
      .fn()
      .mockResolvedValueOnce({
        response: new HttpResponse({ statusCode: 500, headers: {}, body: new Uint8Array() }),
      })
      .mockResolvedValueOnce({
        response: new HttpResponse({ statusCode: 200, headers: {}, body: new Uint8Array() }),
      })
    const store = createStore(handle, 2)

    const result = await store.getClient().headBucket({ Bucket: 'test-bucket' })

    expect(handle).toHaveBeenCalledTimes(2)
    expect(result.$metadata).toMatchObject({ httpStatusCode: 200, attempts: 2 })
  })

  test('preserves SDK errors after removing the logger middleware', async () => {
    const expectedError = new Error('request failed')
    const handle = vi.fn().mockRejectedValue(expectedError)
    const store = createStore(handle)

    await expect(store.getClient().headBucket({ Bucket: 'test-bucket' })).rejects.toBe(
      expectedError
    )
    expect(expectedError).toMatchObject({
      $metadata: { attempts: 1, totalRetryDelay: 0 },
    })
  })
})

describe('S3Store part reader cleanup', () => {
  let server: Server
  let store: TestS3Store
  let sources: fs.ReadStream[]
  let failRequest: boolean
  let contentEncoding: string | undefined

  beforeEach(async () => {
    sources = []
    failRequest = false
    contentEncoding = undefined
    const createReadStream = fs.createReadStream.bind(fs)
    vi.spyOn(fs, 'createReadStream').mockImplementation((...args) => {
      const source = createReadStream(...args)
      sources.push(source)
      return source
    })
    server = createServer((req, res) => {
      contentEncoding = req.headers['content-encoding']
      if (failRequest) {
        res.writeHead(500, { 'content-type': 'application/xml' })
        res.end('<Error><Code>InternalError</Code><Message>part upload failed</Message></Error>')
      } else {
        req.resume()
        req.once('end', () => res.writeHead(200, { etag: '"part-etag"' }).end())
      }
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    store = new TestS3Store({
      partSize: 8 * 1024 * 1024,
      useTags: false,
      s3ClientConfig: {
        bucket: 'test-bucket',
        region: 'us-east-1',
        endpoint: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
        forcePathStyle: true,
        credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
        maxAttempts: 1,
        requestChecksumCalculation: 'WHEN_SUPPORTED',
      },
    })
  })

  afterEach(async () => {
    for (const source of sources) {
      if (!source.closed) {
        await new Promise<void>((resolve) => {
          source.once('close', resolve)
          source.destroy()
        })
      }
      fs.rmSync(source.path, { force: true })
    }
    server.closeAllConnections()
    store.getClient().destroy()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  })

  it.each([
    { bytes: 8 * 1024 * 1024, failure: false },
    { bytes: 8 * 1024 * 1024, failure: true },
    { bytes: 4 * 1024 * 1024, failure: false },
    { bytes: 4 * 1024 * 1024, failure: true },
  ])('closes the $bytes-byte reader after upload, failure=$failure', async ({ bytes, failure }) => {
    failRequest = failure
    const upload = store.uploadPartsForTest(Buffer.alloc(bytes, 'part'))
    if (failure) await expect(upload).rejects.toMatchObject({ name: 'InternalError' })
    else expect(await upload).toBe(bytes)
    expect(contentEncoding).toBe('aws-chunked')
    expect(sources).toHaveLength(1)
    const source = sources[0]
    expect(source.destroyed).toBe(true)
    await vi.waitFor(() => expect(source.closed).toBe(true))
    await vi.waitFor(() => expect(fs.existsSync(source.path)).toBe(false))
  })

  it('preserves buffered part uploads', async () => {
    expect(await store.uploadBufferForTest(Buffer.from('part'))).toBe('"part-etag"')
    expect(sources).toHaveLength(0)
  })
})
