import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { gzipSync } from 'node:zlib'
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3'
import { S3Backend } from '@storage/backend/s3/adapter'
import { getConfig } from '../config'

describe('S3 backend content encoding', () => {
  const bucket = `encoding-${randomUUID()}`
  const keys: string[] = []
  let backend: S3Backend
  let created = false

  beforeAll(async () => {
    const config = getConfig()
    backend = new S3Backend({
      endpoint: config.storageS3Endpoint,
      region: config.storageS3Region,
      forcePathStyle: config.storageS3ForcePathStyle,
    })
    await backend.client.send(new CreateBucketCommand({ Bucket: bucket }))
    created = true
  })

  afterAll(async () => {
    try {
      if (created) {
        await backend.deleteObjects(bucket, keys)
        await backend.client.send(new DeleteBucketCommand({ Bucket: bucket }))
      }
    } finally {
      backend?.client.destroy()
    }
  })

  function key() {
    const name = randomUUID()
    keys.push(name)
    return name
  }

  it('preserves encoding and bytes with a single PutObject request', async () => {
    const name = key()
    const bytes = gzipSync('single-request payload')
    const send = vi.spyOn(backend.client, 'send')
    const metadata = await backend.uploadObject(
      bucket,
      name,
      undefined,
      Readable.from(bytes),
      'application/octet-stream',
      'no-transform',
      undefined,
      bytes.length,
      'gzip'
    )
    expect(send).toHaveBeenCalledTimes(1)
    expect(send.mock.calls[0][0]).toBeInstanceOf(PutObjectCommand)
    expect(metadata.contentEncoding).toBe('gzip')

    const head = await backend.client.send(new HeadObjectCommand({ Bucket: bucket, Key: name }))
    expect(head.ContentEncoding).toBe('gzip')
    expect(head.CacheControl).toBe('no-transform')
    expect(head.ContentLength).toBe(bytes.length)
    const downloaded = await backend.client.send(
      new GetObjectCommand({ Bucket: bucket, Key: name })
    )
    expect(Buffer.from((await downloaded.Body?.transformToByteArray()) ?? [])).toEqual(bytes)
  })

  it.each(['br', undefined])('replaces copied encoding with %s', async (contentEncoding) => {
    const source = key()
    const destination = key()
    const bytes = gzipSync('copied payload')
    await backend.client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: source,
        Body: bytes,
        ContentEncoding: 'gzip',
        ContentType: 'application/octet-stream',
      })
    )

    await backend.copyObject(
      bucket,
      source,
      undefined,
      destination,
      undefined,
      { contentEncoding, cacheControl: 'no-transform', mimetype: 'application/octet-stream' },
      undefined,
      { copyMetadata: false }
    )

    const head = await backend.client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: destination })
    )
    expect(head.ContentEncoding).toBe(contentEncoding)
    expect(head.CacheControl).toBe('no-transform')
    const downloaded = await backend.client.send(
      new GetObjectCommand({ Bucket: bucket, Key: destination })
    )
    expect(Buffer.from((await downloaded.Body?.transformToByteArray()) ?? [])).toEqual(bytes)
  })
})
