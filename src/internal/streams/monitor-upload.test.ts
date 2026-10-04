import { PassThrough, Readable } from 'node:stream'
import { setImmediate } from 'node:timers/promises'
import { S3Client } from '@aws-sdk/client-s3'
import { S3Backend } from '@storage/backend/s3/adapter'

vi.hoisted(() => {
  vi.stubEnv('TRACING_FEATURE_UPLOAD', 'true')
  vi.stubEnv('STORAGE_S3_UPLOAD_PART_SIZE', String(5 * 1024 * 1024))
  vi.stubEnv('STORAGE_S3_UPLOAD_QUEUE_SIZE', '1')
})

afterAll(() => vi.unstubAllEnvs())

it('destroys a traced upload body when the S3 backend rejects multipart creation', async () => {
  const backend = new S3Backend({
    region: 'us-east-1',
    accessKey: 'test',
    secretKey: 'test',
  })
  const originalClient = backend.client
  backend.client = new S3Client({
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    maxAttempts: 1,
    requestHandler: {
      handle: async () => ({
        response: {
          statusCode: 403,
          headers: { 'content-type': 'application/xml' },
          body: Readable.from([
            '<Error><Code>AccessDenied</Code><Message>backend rejected upload</Message></Error>',
          ]),
        },
      }),
    },
  })
  const source = new PassThrough()

  try {
    const upload = backend.uploadObject(
      'bucket',
      'tenant/bucket/object',
      undefined,
      source,
      'application/octet-stream',
      'no-cache'
    )
    // Keep the body open after enough data to start the first multipart request.
    source.write(Buffer.alloc(6 * 1024 * 1024))
    await expect(upload).rejects.toMatchObject({ message: 'AccessDenied' })
    await setImmediate()

    expect(source.destroyed).toBe(true)
  } finally {
    source.destroy()
    backend.client.destroy()
    originalClient.destroy()
  }
})
