import { Readable } from 'node:stream'
import { GetObjectCommand, HeadObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { MAX_HEADER_VALUE_LENGTH } from '@internal/http/header'
import type { HttpRequest } from '@smithy/protocol-http'
import { parseUserMetadata } from '@storage/uploader'
import { S3ProtocolHandler } from './s3-handler'

describe('S3 user metadata responses', () => {
  describe.each(['HEAD', 'GET'] as const)('%s', (method) => {
    it.each([
      {
        name: 'combines case-insensitive metadata collisions',
        metadata: { Title: 'first', title: 'second' },
        expected: { title: 'first,second' },
        missing: undefined,
      },
      {
        name: 'preserves empty values and existing commas when combining three entries',
        metadata: { Title: '', TITLE: 'first,second', title: 'last' },
        expected: { title: ',first,second,last' },
        missing: undefined,
      },
      {
        name: 'keeps distinct names and ordinary values unchanged',
        metadata: { Title: 'first', color: 'blue', empty: '' },
        expected: { title: 'first', color: 'blue', empty: '' },
        missing: undefined,
      },
      {
        name: 'counts invalid entries without overwriting valid colliding entries',
        metadata: { Title: 'first', title: 'second', TITLE: 42, invalid: 'line\nbreak' },
        expected: { title: 'first,second' },
        missing: 2,
      },
      {
        name: 'keeps the existing header length limit when combining entries',
        metadata: { Title: 'a'.repeat(MAX_HEADER_VALUE_LENGTH - 1), title: 'b' },
        expected: { title: 'a'.repeat(MAX_HEADER_VALUE_LENGTH - 1) },
        missing: 1,
      },
    ])('$name', async ({ metadata, expected, missing }) => {
      const client = createClient(metadata)

      try {
        const input = { Bucket: 'bucket', Key: 'key' }
        const result = await client.send(
          method === 'HEAD' ? new HeadObjectCommand(input) : new GetObjectCommand(input)
        )
        expect(result.MissingMeta).toBe(missing)
        expect(result.Metadata).toEqual(expected)
        if ('Body' in result && result.Body) {
          await result.Body.transformToString()
        }
      } finally {
        client.destroy()
      }
    })
  })
})

function createClient(metadata: Record<string, unknown>) {
  // Standard Storage uploads accept JSON metadata through the x-metadata header.
  const userMetadata = parseUserMetadata(Buffer.from(JSON.stringify(metadata)).toString('base64'))
  const objectMetadata = {
    cacheControl: 'no-cache',
    contentLength: 0,
    eTag: '"etag"',
    lastModified: new Date('2026-01-01T00:00:00Z'),
    mimetype: 'text/plain',
    size: 0,
  }
  const handler = new S3ProtocolHandler(
    {
      from: () => ({
        findObject: async () => ({
          metadata: objectMetadata,
          user_metadata: userMetadata,
          version: 'version',
          updated_at: '2026-01-01T00:00:00Z',
        }),
      }),
      backend: {
        getObject: async () => ({ httpStatusCode: 200, metadata: objectMetadata }),
      },
      location: {
        getRootLocation: () => 'root-bucket',
        getKeyLocation: () => 'tenant/bucket/key',
      },
    } as never,
    'tenant'
  )

  return new S3Client({
    region: 'us-east-1',
    endpoint: 'http://storage.test/s3',
    forcePathStyle: true,
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    maxAttempts: 1,
    requestHandler: {
      handle: async (request: HttpRequest) => {
        const command = { Bucket: 'bucket', Key: 'key' }
        const result =
          request.method === 'HEAD'
            ? await handler.dbHeadObject(command)
            : await handler.getObject(command)

        return {
          response: {
            statusCode: 200,
            headers: Object.fromEntries(
              Object.entries(result.headers)
                .filter(([name, value]) => value || name.startsWith('x-amz-meta-'))
                .map(([name, value]) => [name, String(value)])
            ),
            body: Readable.from([]),
          },
        }
      },
    },
  })
}
