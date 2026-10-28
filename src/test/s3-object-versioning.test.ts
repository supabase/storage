import {
  CompleteMultipartUploadCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetBucketVersioningCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListObjectVersionsCommand,
  ListObjectVersionsCommandOutput,
  PutBucketVersioningCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
  UploadPartCommand,
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig, mergeConfig } from '../config'

const { s3ProtocolAccessKeySecret, s3ProtocolAccessKeyId, storageS3Region } = getConfig()

/**
 * Wire-level S3 versioning behaviour, matched against responses recorded from
 * AWS S3.
 */
describe('S3 object versioning', () => {
  let testApp: FastifyInstance
  let client: S3Client

  beforeAll(async () => {
    testApp = app()
    const listener = await testApp.listen()
    client = new S3Client({
      endpoint: `${listener.replace('[::1]', 'localhost')}/s3`,
      forcePathStyle: true,
      region: storageS3Region,
      credentials: {
        accessKeyId: s3ProtocolAccessKeyId!,
        secretAccessKey: s3ProtocolAccessKeySecret!,
      },
    })
  })

  beforeEach(() => {
    mergeConfig({ storageVersioningEnabled: true })
  })

  afterEach(() => {
    getConfig({ reload: true })
  })

  afterAll(async () => {
    client.destroy()
    await testApp.close()
  })

  async function createBucket(versioning?: 'Enabled' | 'Suspended') {
    const bucket = `versioning-${randomUUID()}`
    await client.send(new CreateBucketCommand({ Bucket: bucket }))
    if (versioning) {
      await client.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: versioning },
        })
      )
    }
    return bucket
  }

  async function put(bucket: string, key: string, body: string) {
    return client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }))
  }

  async function body(bucket: string, key: string, versionId?: string) {
    const response = await client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key, VersionId: versionId })
    )
    return response.Body!.transformToString()
  }

  /** Sends a presigned request so the raw status, headers and body can be asserted. */
  async function raw(signedUrl: Promise<string>, method = 'GET') {
    const response = await fetch(await signedUrl, {
      method,
      headers: { accept: 'application/xml' },
    })
    return {
      status: response.status,
      headers: response.headers,
      body: (await response.text()).replace(/^<\?xml[^>]*\?>/, ''),
    }
  }

  async function sendError(request: Promise<unknown>) {
    return request.then(
      () => {
        throw new Error('expected the request to fail')
      },
      (e: S3ServiceException) => e
    )
  }

  async function listAllVersions(bucket: string, params: { Prefix?: string; Delimiter?: string }) {
    const pages: ListObjectVersionsCommandOutput[] = []
    let KeyMarker: string | undefined
    let VersionIdMarker: string | undefined
    do {
      const page = await client.send(
        new ListObjectVersionsCommand({
          Bucket: bucket,
          MaxKeys: 1,
          KeyMarker,
          VersionIdMarker,
          ...params,
        })
      )
      pages.push(page)
      KeyMarker = page.NextKeyMarker
      VersionIdMarker = page.NextVersionIdMarker
      if (!page.IsTruncated) {
        break
      }
    } while (pages.length < 50)
    return pages
  }

  describe('bucket versioning configuration', () => {
    it('reports a never-configured bucket as an empty configuration', async () => {
      const bucket = await createBucket()

      const response = await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))
      expect(response.Status).toBeUndefined()
      expect(response.MFADelete).toBeUndefined()

      const rawResponse = await raw(
        getSignedUrl(client, new GetBucketVersioningCommand({ Bucket: bucket }), { expiresIn: 60 })
      )
      expect(rawResponse.body).toBe(
        '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>'
      )
    })

    it('enables and suspends versioning through PutBucketVersioning', async () => {
      const bucket = await createBucket('Enabled')
      expect((await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))).Status).toBe(
        'Enabled'
      )

      await client.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: 'Suspended' },
        })
      )
      expect((await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))).Status).toBe(
        'Suspended'
      )
    })

    it('rejects an unknown versioning status as MalformedXML', async () => {
      const bucket = await createBucket()

      const error = await sendError(
        client.send(
          new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Disabled' as 'Enabled' },
          })
        )
      )
      expect(error.name).toBe('MalformedXML')
      expect(error.$metadata.httpStatusCode).toBe(400)
    })
  })

  describe('writes', () => {
    const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

    async function completeMultipart(bucket: string, key: string) {
      const { UploadId } = await client.send(
        new CreateMultipartUploadCommand({ Bucket: bucket, Key: key })
      )
      const part = await client.send(
        new UploadPartCommand({
          Bucket: bucket,
          Key: key,
          UploadId,
          PartNumber: 1,
          Body: Buffer.from('part-1'),
        })
      )
      return client.send(
        new CompleteMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          UploadId,
          MultipartUpload: { Parts: [{ PartNumber: 1, ETag: part.ETag }] },
        })
      )
    }

    it('returns a new version id for each write while versioning is enabled', async () => {
      const bucket = await createBucket('Enabled')

      const first = (await put(bucket, 'obj.txt', 'one')).VersionId
      const second = (await put(bucket, 'obj.txt', 'two')).VersionId
      const multipart = (await completeMultipart(bucket, 'obj.txt')).VersionId

      for (const versionId of [first, second, multipart]) {
        expect(versionId).toMatch(UUID)
      }
      expect(new Set([first, second, multipart]).size).toBe(3)
    })

    it('returns no version id for writes to the null version', async () => {
      const neverVersioned = await createBucket()
      const suspended = await createBucket('Enabled')
      await client.send(
        new PutBucketVersioningCommand({
          Bucket: suspended,
          VersioningConfiguration: { Status: 'Suspended' },
        })
      )

      for (const bucket of [neverVersioned, suspended]) {
        expect((await put(bucket, 'obj.txt', 'one')).VersionId).toBeUndefined()
        expect((await completeMultipart(bucket, 'obj.txt')).VersionId).toBeUndefined()
      }
    })
  })

  describe('reads', () => {
    it('returns version ids on writes and reads, and serves a noncurrent version', async () => {
      const bucket = await createBucket('Enabled')
      const v1 = (await put(bucket, 'obj.txt', 'version-1')).VersionId
      const v2 = (await put(bucket, 'obj.txt', 'version-2')).VersionId
      expect(v1).toMatch(/^[0-9a-f-]{36}$/)
      expect(v2).toMatch(/^[0-9a-f-]{36}$/)
      expect(v2).not.toBe(v1)

      const current = await client.send(new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      expect(current.VersionId).toBe(v2)
      expect(await current.Body!.transformToString()).toBe('version-2')

      expect(await body(bucket, 'obj.txt', v1)).toBe('version-1')
      const head = await client.send(
        new HeadObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: v1 })
      )
      expect(head.VersionId).toBe(v1)
      expect(head.ContentLength).toBe('version-1'.length)
    })

    it('names the null version on reads only once versioning has been configured', async () => {
      const neverVersioned = await createBucket()
      await put(neverVersioned, 'obj.txt', 'content')
      const read = await client.send(
        new GetObjectCommand({ Bucket: neverVersioned, Key: 'obj.txt', VersionId: 'null' })
      )
      expect(read.VersionId).toBeUndefined()
      expect(await read.Body!.transformToString()).toBe('content')

      const suspended = await createBucket('Enabled')
      await client.send(
        new PutBucketVersioningCommand({
          Bucket: suspended,
          VersioningConfiguration: { Status: 'Suspended' },
        })
      )
      await put(suspended, 'obj.txt', 'content')
      const current = await client.send(new GetObjectCommand({ Bucket: suspended, Key: 'obj.txt' }))
      expect(current.VersionId).toBe('null')
      const head = await client.send(
        new HeadObjectCommand({ Bucket: suspended, Key: 'obj.txt', VersionId: 'null' })
      )
      expect(head.VersionId).toBe('null')
    })

    it('reports missing and malformed version ids as S3 does', async () => {
      const bucket = await createBucket('Enabled')
      await put(bucket, 'obj.txt', 'content')

      const missing = await sendError(
        client.send(
          new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: randomUUID() })
        )
      )
      expect(missing.name).toBe('NoSuchVersion')
      expect(missing.$metadata.httpStatusCode).toBe(404)

      const noNullVersion = await sendError(
        client.send(new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'null' }))
      )
      expect(noNullVersion.name).toBe('NoSuchVersion')

      const malformed = await sendError(
        client.send(
          new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'not-a-version' })
        )
      )
      expect(malformed.name).toBe('InvalidArgument')
      expect(malformed.message).toBe('Invalid version id specified')
      expect(malformed.$metadata.httpStatusCode).toBe(400)
    })

    it('answers 404 with the marker headers when the current version is a delete marker', async () => {
      const bucket = await createBucket('Enabled')
      await put(bucket, 'obj.txt', 'content')
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      const listed = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      const markerId = listed.DeleteMarkers?.[0].VersionId

      const get = await raw(
        getSignedUrl(client, new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt' }), {
          expiresIn: 60,
        })
      )
      expect(get.status).toBe(404)
      expect(get.headers.get('x-amz-delete-marker')).toBe('true')
      expect(get.headers.get('x-amz-version-id')).toBe(markerId)
      expect(get.body).toContain('<Code>NoSuchKey</Code>')

      const head = await raw(
        getSignedUrl(client, new HeadObjectCommand({ Bucket: bucket, Key: 'obj.txt' }), {
          expiresIn: 60,
        }),
        'HEAD'
      )
      expect(head.status).toBe(404)
      expect(head.headers.get('x-amz-delete-marker')).toBe('true')
      expect(head.headers.get('x-amz-version-id')).toBe(markerId)
    })

    it('answers 405 MethodNotAllowed when a delete marker is addressed by version id', async () => {
      const bucket = await createBucket('Enabled')
      await put(bucket, 'obj.txt', 'content')
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      const listed = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      const markerId = listed.DeleteMarkers?.[0].VersionId

      const target = { Bucket: bucket, Key: 'obj.txt', VersionId: markerId }
      for (const [signedUrl, method] of [
        [getSignedUrl(client, new GetObjectCommand(target), { expiresIn: 60 }), 'GET'],
        [getSignedUrl(client, new HeadObjectCommand(target), { expiresIn: 60 }), 'HEAD'],
      ] as const) {
        const response = await raw(signedUrl, method)
        expect(response.status).toBe(405)
        expect(response.headers.get('x-amz-delete-marker')).toBe('true')
        expect(response.headers.get('x-amz-version-id')).toBe(markerId)
        expect(response.headers.get('allow')).toBe('DELETE')
        expect(response.headers.get('last-modified')).toBeTruthy()
        if (method === 'GET') {
          expect(response.body).toContain('<Code>MethodNotAllowed</Code>')
        }
      }
    })
  })

  describe('DeleteObject', () => {
    it('writes delete markers, removes them by version id, and promotes the next version', async () => {
      const bucket = await createBucket('Enabled')
      const v1 = (await put(bucket, 'obj.txt', 'version-1')).VersionId
      const v2 = (await put(bucket, 'obj.txt', 'version-2')).VersionId

      const marker1 = await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      expect(marker1.DeleteMarker).toBe(true)
      expect(marker1.VersionId).toMatch(/^[0-9a-f-]{36}$/)

      // A simple delete on a key whose current version is a marker stacks another one.
      const marker2 = await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      expect(marker2.DeleteMarker).toBe(true)
      expect(marker2.VersionId).not.toBe(marker1.VersionId)

      for (const marker of [marker2, marker1]) {
        const removed = await client.send(
          new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: marker.VersionId })
        )
        expect(removed.DeleteMarker).toBe(true)
        expect(removed.VersionId).toBe(marker.VersionId)
      }
      expect(await body(bucket, 'obj.txt')).toBe('version-2')

      const removedV2 = await raw(
        getSignedUrl(
          client,
          new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: v2 }),
          { expiresIn: 60 }
        ),
        'DELETE'
      )
      expect(removedV2.status).toBe(204)
      expect(removedV2.headers.get('x-amz-version-id')).toBe(v2)
      expect(removedV2.headers.get('x-amz-delete-marker')).toBeNull()
      expect(await body(bucket, 'obj.txt')).toBe('version-1')

      // Deleting a version that no longer exists is a successful no-op.
      const again = await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: v2 })
      )
      expect(again.VersionId).toBe(v2)
      expect(await body(bucket, 'obj.txt', v1)).toBe('version-1')
    })

    it('writes a delete marker for a key that never existed', async () => {
      const bucket = await createBucket('Enabled')

      const marker = await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: 'never.txt' })
      )
      expect(marker.DeleteMarker).toBe(true)

      const versions = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      expect(versions.DeleteMarkers).toEqual([
        expect.objectContaining({ Key: 'never.txt', VersionId: marker.VersionId, IsLatest: true }),
      ])
    })

    it('rejects a malformed version id', async () => {
      const bucket = await createBucket('Enabled')

      const error = await sendError(
        client.send(
          new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'not-a-version' })
        )
      )
      expect(error.name).toBe('InvalidArgument')
    })
  })

  describe('DeleteObjects', () => {
    it('reports each entry the way S3 does', async () => {
      const bucket = await createBucket('Enabled')
      const b1v1 = (await put(bucket, 'b1.txt', 'b1-v1')).VersionId
      await put(bucket, 'b1.txt', 'b1-v2')
      await put(bucket, 'b2.txt', 'b2-v1')
      const b3Marker = (
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'b3.txt' }))
      ).VersionId
      const missingVersion = randomUUID()

      const response = await client.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: {
            Objects: [
              { Key: 'b2.txt' },
              { Key: 'b1.txt', VersionId: b1v1 },
              { Key: 'b3.txt', VersionId: b3Marker },
              { Key: 'b4.txt', VersionId: missingVersion },
              { Key: 'b4.txt', VersionId: 'null' },
              { Key: 'b4.txt', VersionId: 'not-a-version' },
            ],
          },
        })
      )

      expect(response.Deleted).toEqual([
        {
          Key: 'b2.txt',
          DeleteMarker: true,
          DeleteMarkerVersionId: expect.stringMatching(/^[0-9a-f-]{36}$/),
        },
        { Key: 'b1.txt', VersionId: b1v1 },
        { Key: 'b3.txt', VersionId: b3Marker, DeleteMarker: true, DeleteMarkerVersionId: b3Marker },
        { Key: 'b4.txt', VersionId: missingVersion },
        { Key: 'b4.txt', VersionId: 'null' },
      ])
      expect(response.Errors).toEqual([
        {
          Key: 'b4.txt',
          VersionId: 'not-a-version',
          Code: 'NoSuchVersion',
          Message: 'The specified version does not exist.',
        },
      ])

      const versions = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      expect(versions.Versions?.map((v) => [v.Key, v.IsLatest])).toEqual([
        ['b1.txt', true],
        ['b2.txt', false],
      ])
      expect(versions.DeleteMarkers?.map((m) => m.Key)).toEqual(['b2.txt'])
    })
  })

  describe('ListObjectVersions', () => {
    async function seedListing(bucket: string) {
      const a = []
      for (let i = 0; i < 3; i++) {
        a.push((await put(bucket, 'list/a.txt', `a-${i}`)).VersionId!)
      }
      const b = (await put(bucket, 'list/b.txt', 'b')).VersionId!
      const bMarker = (
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'list/b.txt' }))
      ).VersionId!
      await put(bucket, 'list/dir1/x.txt', 'x')
      await put(bucket, 'list/dir1/y.txt', 'y')
      await put(bucket, 'list/dir2/z.txt', 'z')
      const dir3Marker = (
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'list/dir3/gone.txt' }))
      ).VersionId!
      return { a: a.reverse(), b, bMarker, dir3Marker }
    }

    it('lists keys ascending, each key newest first, with markers interleaved', async () => {
      const bucket = await createBucket('Enabled')
      const { a, b, bMarker, dir3Marker } = await seedListing(bucket)

      const rawList = await raw(
        getSignedUrl(client, new ListObjectVersionsCommand({ Bucket: bucket, Prefix: 'list/' }), {
          expiresIn: 60,
        })
      )
      expect(rawList.status).toBe(200)
      const order = [
        ...rawList.body.matchAll(/<(Version|DeleteMarker)><Key>([^<]*)<\/Key><VersionId>([^<]*)</g),
      ].map((match) => [match[1], match[2], match[3]])
      expect(order).toEqual([
        ['Version', 'list/a.txt', a[0]],
        ['Version', 'list/a.txt', a[1]],
        ['Version', 'list/a.txt', a[2]],
        ['DeleteMarker', 'list/b.txt', bMarker],
        ['Version', 'list/b.txt', b],
        ['Version', 'list/dir1/x.txt', expect.any(String)],
        ['Version', 'list/dir1/y.txt', expect.any(String)],
        ['Version', 'list/dir2/z.txt', expect.any(String)],
        ['DeleteMarker', 'list/dir3/gone.txt', dir3Marker],
      ])
      expect(rawList.body).toMatch(
        /^<ListVersionsResult xmlns="http:\/\/s3.amazonaws.com\/doc\/2006-03-01\/"><Name>[^<]+<\/Name><Prefix>list\/<\/Prefix><KeyMarker\/><VersionIdMarker\/><MaxKeys>1000<\/MaxKeys><IsTruncated>false<\/IsTruncated><Version>/
      )

      const listed = await client.send(
        new ListObjectVersionsCommand({ Bucket: bucket, Prefix: 'list/' })
      )
      expect(listed.Versions?.find((v) => v.VersionId === a[0])).toMatchObject({
        IsLatest: true,
        Size: 3,
        StorageClass: 'STANDARD',
      })
      expect(listed.Versions?.find((v) => v.VersionId === a[1])?.IsLatest).toBe(false)
      expect(listed.DeleteMarkers?.find((m) => m.VersionId === bMarker)?.IsLatest).toBe(true)

      // ListObjectsV2 hides keys whose current version is a delete marker.
      const current = await client.send(
        new ListObjectsV2Command({ Bucket: bucket, Prefix: 'list/' })
      )
      expect(current.Contents?.map((o) => o.Key)).toEqual([
        'list/a.txt',
        'list/dir1/x.txt',
        'list/dir1/y.txt',
        'list/dir2/z.txt',
      ])
    })

    it('pages one entry at a time with the last returned entry as the next marker', async () => {
      const bucket = await createBucket('Enabled')
      const { a, b, bMarker } = await seedListing(bucket)

      const pages = await listAllVersions(bucket, { Prefix: 'list/' })
      expect(pages).toHaveLength(9)
      expect(pages[0]).toMatchObject({
        IsTruncated: true,
        NextKeyMarker: 'list/a.txt',
        NextVersionIdMarker: a[0],
      })
      expect(pages[1]).toMatchObject({
        KeyMarker: 'list/a.txt',
        VersionIdMarker: a[0],
        NextKeyMarker: 'list/a.txt',
        NextVersionIdMarker: a[1],
      })
      expect(pages[3].DeleteMarkers?.[0].VersionId).toBe(bMarker)
      expect(pages[4].Versions?.[0].VersionId).toBe(b)
      expect(pages[8].IsTruncated).toBe(false)
      expect(pages[8].NextKeyMarker).toBeUndefined()
      expect(pages[8].NextVersionIdMarker).toBeUndefined()
    })

    it('resumes after a key, after a version, or after a common prefix', async () => {
      const bucket = await createBucket('Enabled')
      const { a } = await seedListing(bucket)

      const afterKey = await client.send(
        new ListObjectVersionsCommand({ Bucket: bucket, Prefix: 'list/', KeyMarker: 'list/a.txt' })
      )
      expect(afterKey.Versions?.some((v) => v.Key === 'list/a.txt')).toBe(false)

      const afterNewest = await client.send(
        new ListObjectVersionsCommand({
          Bucket: bucket,
          Prefix: 'list/',
          KeyMarker: 'list/a.txt',
          VersionIdMarker: a[0],
        })
      )
      expect(
        afterNewest.Versions?.filter((v) => v.Key === 'list/a.txt').map((v) => v.VersionId)
      ).toEqual([a[1], a[2]])

      const delimited = await client.send(
        new ListObjectVersionsCommand({ Bucket: bucket, Prefix: 'list/', Delimiter: '/' })
      )
      expect(delimited.CommonPrefixes?.map((p) => p.Prefix)).toEqual([
        'list/dir1/',
        'list/dir2/',
        'list/dir3/',
      ])
      expect(delimited.Versions?.map((v) => v.Key)).toEqual([
        'list/a.txt',
        'list/a.txt',
        'list/a.txt',
        'list/b.txt',
      ])

      const paged = await listAllVersions(bucket, { Prefix: 'list/', Delimiter: '/' })
      const prefixPage = paged.find((page) => page.CommonPrefixes?.[0]?.Prefix === 'list/dir1/')
      expect(prefixPage).toMatchObject({ NextKeyMarker: 'list/dir1/', IsTruncated: true })
      expect(prefixPage?.NextVersionIdMarker).toBeUndefined()
      expect(paged.flatMap((page) => page.CommonPrefixes?.map((p) => p.Prefix) ?? [])).toEqual([
        'list/dir1/',
        'list/dir2/',
        'list/dir3/',
      ])
    })

    it('validates markers and max-keys', async () => {
      const bucket = await createBucket('Enabled')

      const withoutKeyMarker = await sendError(
        client.send(
          new ListObjectVersionsCommand({ Bucket: bucket, VersionIdMarker: randomUUID() })
        )
      )
      expect(withoutKeyMarker.name).toBe('InvalidArgument')
      expect(withoutKeyMarker.message).toBe(
        'A version-id marker cannot be specified without a key marker.'
      )

      const malformed = await sendError(
        client.send(
          new ListObjectVersionsCommand({
            Bucket: bucket,
            KeyMarker: 'a.txt',
            VersionIdMarker: 'not-a-version',
          })
        )
      )
      expect(malformed.name).toBe('InvalidArgument')

      const negative = await sendError(
        client.send(new ListObjectVersionsCommand({ Bucket: bucket, MaxKeys: -1 }))
      )
      expect(negative.name).toBe('InvalidArgument')
      expect(negative.message).toBe('max-keys cannot be negative')

      await put(bucket, 'a.txt', 'a')
      const empty = await client.send(new ListObjectVersionsCommand({ Bucket: bucket, MaxKeys: 0 }))
      expect(empty.IsTruncated).toBe(false)
      expect(empty.Versions).toBeUndefined()
    })

    it('url-encodes keys when asked to', async () => {
      const bucket = await createBucket('Enabled')
      await put(bucket, 'weird key &+(1).txt', 'w')

      const rawList = await raw(
        getSignedUrl(
          client,
          new ListObjectVersionsCommand({ Bucket: bucket, EncodingType: 'url' }),
          { expiresIn: 60 }
        )
      )
      expect(rawList.body).toContain('<Key>weird%20key%20%26%2B%281%29.txt</Key>')
      expect(rawList.body).toContain('<EncodingType>url</EncodingType><IsTruncated>')
    })
  })

  describe('buckets without versioning', () => {
    it('deletes the null version by version id', async () => {
      const bucket = await createBucket()
      await put(bucket, 'obj.txt', 'content')

      const deleted = await raw(
        getSignedUrl(
          client,
          new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'null' }),
          { expiresIn: 60 }
        ),
        'DELETE'
      )
      expect(deleted.status).toBe(204)
      expect(deleted.headers.get('x-amz-version-id')).toBe('null')
      expect(deleted.headers.get('x-amz-delete-marker')).toBeNull()

      const gone = await sendError(
        client.send(new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      )
      expect(gone.name).toBe('NoSuchKey')
    })

    it('deletes without writing delete markers', async () => {
      const bucket = await createBucket()
      await put(bucket, 'obj.txt', 'content')

      const deleted = await raw(
        getSignedUrl(client, new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }), {
          expiresIn: 60,
        }),
        'DELETE'
      )
      expect(deleted.status).toBe(204)
      expect(deleted.headers.get('x-amz-delete-marker')).toBeNull()
      expect(deleted.headers.get('x-amz-version-id')).toBeNull()

      const listed = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      expect(listed.Versions).toBeUndefined()
      expect(listed.DeleteMarkers).toBeUndefined()
    })

    it('pages null versions with a null version-id marker', async () => {
      const bucket = await createBucket()
      await put(bucket, 'a.txt', 'a')
      await put(bucket, 'b.txt', 'b')

      const first = await client.send(new ListObjectVersionsCommand({ Bucket: bucket, MaxKeys: 1 }))
      expect(first).toMatchObject({
        IsTruncated: true,
        NextKeyMarker: 'a.txt',
        NextVersionIdMarker: 'null',
      })
      const second = await client.send(
        new ListObjectVersionsCommand({
          Bucket: bucket,
          MaxKeys: 1,
          KeyMarker: first.NextKeyMarker,
          VersionIdMarker: first.NextVersionIdMarker,
        })
      )
      expect(second.Versions?.map((v) => v.Key)).toEqual(['b.txt'])
      expect(second.IsTruncated).toBe(false)
    })
  })

  describe('suspended versioning', () => {
    it('keeps one null version beside the real ones and deletes it with a null marker', async () => {
      const bucket = await createBucket('Enabled')
      const v1 = (await put(bucket, 'obj.txt', 'versioned')).VersionId
      await client.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: 'Suspended' },
        })
      )

      const nullWrite = await put(bucket, 'obj.txt', 'null-1')
      expect(nullWrite.VersionId).toBeUndefined()
      await put(bucket, 'obj.txt', 'null-2')
      expect(await body(bucket, 'obj.txt', 'null')).toBe('null-2')

      // Reads name the null version, unlike writes.
      const current = await client.send(new GetObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      expect(current.VersionId).toBe('null')
      const head = await client.send(
        new HeadObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'null' })
      )
      expect(head.VersionId).toBe('null')

      let listed = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      expect(listed.Versions?.map((v) => [v.VersionId, v.IsLatest])).toEqual([
        ['null', true],
        [v1, false],
      ])

      const marker = await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))
      expect(marker.DeleteMarker).toBe(true)
      expect(marker.VersionId).toBe('null')

      listed = await client.send(new ListObjectVersionsCommand({ Bucket: bucket }))
      expect(listed.DeleteMarkers?.map((m) => [m.VersionId, m.IsLatest])).toEqual([['null', true]])
      expect(listed.Versions?.map((v) => v.VersionId)).toEqual([v1])

      const removed = await client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt', VersionId: 'null' })
      )
      expect(removed.DeleteMarker).toBe(true)
      expect(removed.VersionId).toBe('null')
      expect(await body(bucket, 'obj.txt')).toBe('versioned')
    })
  })

  it('reports a missing bucket before a missing object on reads', async () => {
    const error = await sendError(
      client.send(new GetObjectCommand({ Bucket: `missing-${randomUUID()}`, Key: 'obj.txt' }))
    )
    expect(error.name).toBe('NoSuchBucket')
  })

  it('allows an If-None-Match: * write over a delete marker', async () => {
    const bucket = await createBucket('Enabled')
    await put(bucket, 'obj.txt', 'first')
    await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: 'obj.txt' }))

    const written = await client.send(
      new PutObjectCommand({ Bucket: bucket, Key: 'obj.txt', Body: 'second', IfNoneMatch: '*' })
    )
    expect(written.VersionId).toMatch(/^[0-9a-f-]{36}$/)
  })
})
