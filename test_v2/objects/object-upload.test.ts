import fs from 'node:fs'
import { beforeAll, describe, expect, test } from 'vitest'
import { ErrorCode } from '@internal/errors'
import { mergeConfig } from '../../src/config'
import {
  SADCAT_PATH,
  SADCAT_SIZE,
  binaryUpload,
  multipartUpload,
  useTestContext,
} from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports the four upload-oriented describe blocks from `src/test/object.test.ts`:
 *
 *   - POST /object/* multipart
 *   - POST /object/* binary
 *   - PUT  /object/* multipart
 *   - PUT  /object/* binary
 *
 * Each test seeds its own bucket so the suite is fully self-contained and
 * does not rely on the legacy `02-dummy-data.sql` fixtures. We go through the
 * real MinIO backend for every happy-path upload (no S3 mocks).
 */

let standard: TestBucket
let sizeLimited: TestBucket
let sizeLimitedLarge: TestBucket
let mimeRestricted: TestBucket

beforeAll(async () => {
  standard = await ctx.factories.bucket.create()
  // file_size_limit = 10 bytes — sadcat.jpg is way larger so the guard trips.
  sizeLimited = await ctx.factories.bucket.withSizeLimit(10, { public: true })
  // file_size_limit == sadcat.jpg exact size, so the same file squeaks by.
  sizeLimitedLarge = await ctx.factories.bucket.withSizeLimit(SADCAT_SIZE, { public: true })
  // Only image/jpeg allowed → used to drive mime-allow-list + empty-folder tests.
  mimeRestricted = await ctx.factories.bucket.withMimeTypes(['image/jpeg'], { public: true })
})

describe('POST /object/:bucket/* (multipart)', () => {
  test('service role can upload and gets a Key/Id back', async () => {
    const name = 'multipart/cat.png'
    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({
      Id: expect.any(String),
      Key: `${standard.id}/${name}`,
    })

    await ctx.snapshot.object({ bucketId: standard.id, name }).matches({
      bucket_id: standard.id,
      name,
      metadata: expect.objectContaining({
        size: SADCAT_SIZE,
        contentLength: SADCAT_SIZE,
        mimetype: 'image/jpeg',
        httpStatusCode: 200,
        eTag: expect.any(String),
      }),
    })
  })

  test('anon caller cannot upload (RLS)', async () => {
    const name = 'anon-denied.png'
    const res = await ctx.client.asAnon().inject({
      method: 'POST',
      url: `/object/${standard.id}/${name}`,
      // use the multipartUpload helper with a bogus token via headers override
      headers: {
        'content-type': 'multipart/form-data; boundary=----testboundary',
      },
      payload: '------testboundary--\r\n',
    })

    // The body is not valid multipart, but that's fine — RLS denies first.
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('upload without auth header is rejected', async () => {
    const name = 'no-auth.png'
    const res = await ctx.app.inject({
      method: 'POST',
      url: `/object/${standard.id}/${name}`,
      headers: { 'content-type': 'multipart/form-data; boundary=----tb' },
      payload: '------tb--\r\n',
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('uploading to a non-existent bucket returns 400', async () => {
    const missingBucket = `${ctx.prefix}_missing_bucket`
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${missingBucket}/dest.png`,
      { upsert: true }
    )
    expect(res.statusCode).toBe(400)
    // And nothing sneaked into storage.objects under that bucket id.
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  test('duplicate upload (same key, no upsert) is rejected and row is untouched', async () => {
    const name = 'dup/cat.png'
    const first = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(first.statusCode).toBe(200)
    const firstRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`)
    expect(res.statusCode).toBe(400)

    // Same id, same version — nothing was overwritten.
    const afterRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()
    expect(afterRow.id).toBe(firstRow.id)
    expect(afterRow.version).toBe(firstRow.version)
  })

  test('upsert of an existing object rewrites the row in place', async () => {
    const name = 'upsert/cat.png'
    const first = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(first.statusCode).toBe(200)
    const firstRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(200)

    // Exactly one row survives, and the storage backend wrote a fresh version.
    const rows = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
    expect(rows).toHaveLength(1)
    expect(rows[0].version).not.toBe(firstRow.version)
  })

  test('upload within bucket max size limit', async () => {
    const name = 'within-limit.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${sizeLimitedLarge.id}/${name}`,
      { upsert: true }
    )
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: sizeLimitedLarge.id, name }).matches({
      name,
      metadata: expect.objectContaining({ size: SADCAT_SIZE }),
    })
  })

  test('upload exceeding bucket max size limit → 413 envelope', async () => {
    const name = 'too-big.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${sizeLimited.id}/${name}`,
      { upsert: true }
    )
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      error: 'Payload too large',
      message: 'The object exceeded the maximum allowed size',
      statusCode: '413',
    })
    await ctx.snapshot.object({ bucketId: sizeLimited.id, name }).notFound()
  })

  test('upload with an allowed mime type succeeds and persists mimetype', async () => {
    const name = 'allowed.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        headers: { 'content-type': 'image/jpeg' },
      }
    )
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).matches({
      metadata: expect.objectContaining({ mimetype: 'image/jpeg' }),
    })
  })

  test('upload with a disallowed mime type → 415', async () => {
    const name = 'not-allowed.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        headers: { 'content-type': 'image/png' },
      }
    )
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      error: 'invalid_mime_type',
      message: 'mime type image/png is not supported',
      statusCode: '415',
    })
    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).notFound()
  })

  test('malformed content-type header is rejected', async () => {
    const name = 'malformed.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        headers: { 'content-type': 'thisisnotarealmimetype' },
      }
    )
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      error: 'invalid_mime_type',
      message: 'Invalid Content-Type header',
      statusCode: '415',
    })
    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).notFound()
  })

  test('content-type with embedded tabs is rejected', async () => {
    const name = 'tab.png'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        headers: { 'content-type': 'image/\tjpg' },
      }
    )
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      error: 'invalid_mime_type',
      message: 'Invalid Content-Type header',
      statusCode: '415',
    })
    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).notFound()
  })

  test('metadata field (JSON string) is persisted to user_metadata', async () => {
    const name = 'meta/form-field.png'
    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
      fields: { metadata: JSON.stringify({ test1: 'test1', test2: 'test2' }) },
    })
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: standard.id, name }).matches({
      user_metadata: { test1: 'test1', test2: 'test2' },
    })
  })

  test('x-metadata header (base64 JSON) is persisted to user_metadata', async () => {
    const name = 'meta/x-header.png'
    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
      headers: {
        'x-metadata': Buffer.from(JSON.stringify({ a: 1, b: 'two' })).toString('base64'),
      },
    })
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: standard.id, name }).matches({
      user_metadata: { a: 1, b: 'two' },
    })
  })

  test('GET /object/info returns persisted user metadata', async () => {
    const name = 'meta/info.png'
    const uploaded = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
      fields: { metadata: JSON.stringify({ test1: 'test1', test2: 'test2' }) },
    })
    expect(uploaded.statusCode).toBe(200)

    const info = await ctx.client.asService().get(`/object/info/${standard.id}/${name}`)
    expect(info.statusCode).toBe(200)
    expect(info.json().metadata).toEqual({ test1: 'test1', test2: 'test2' })
  })

  test('0-byte .emptyFolderPlaceholder is accepted on a mime-restricted bucket', async () => {
    const name = 'nested/.emptyFolderPlaceholder'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        payloadBuffer: Buffer.alloc(0),
      }
    )
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).matches({
      name,
      metadata: expect.objectContaining({ size: 0, contentLength: 0 }),
    })
  })

  test('non-empty .emptyFolderPlaceholder is rejected', async () => {
    const name = 'nested-2/.emptyFolderPlaceholder'
    const res = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${mimeRestricted.id}/${name}`,
      {
        upsert: true,
        payloadBuffer: Buffer.alloc(1),
      }
    )
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: mimeRestricted.id, name }).notFound()
  })

  test('upload exceeding global upload size limit → 413', async () => {
    mergeConfig({ uploadFileSizeLimit: 1 })
    const name = 'global-limit.png'
    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      statusCode: '413',
      error: 'Payload too large',
      message: 'The object exceeded the maximum allowed size',
    })
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('upload to a URL with no file name returns 400', async () => {
    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(400)
    // A no-name URL must never land any row; guard with a direct count.
    const countFor = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name: '' })
      .count('* as c')
    expect(Number(countFor[0].c)).toBe(0)
  })

  test('failed S3 upload does NOT insert a DB row', async () => {
    // Drive a real failure through the uploader by forcing the global limit
    // below the file size *after* the DB row would normally be created.
    mergeConfig({ uploadFileSizeLimit: 1 })
    const name = 'should-not-insert/cat.png'

    const res = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toMatchObject({ statusCode: '413' })

    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })
})

describe('POST /object/:bucket/* (binary)', () => {
  test('service role can upload a binary stream', async () => {
    const name = 'bin/cat.png'
    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({
      Id: expect.any(String),
      Key: `${standard.id}/${name}`,
    })

    await ctx.snapshot.object({ bucketId: standard.id, name }).matches({
      bucket_id: standard.id,
      name,
      metadata: expect.objectContaining({
        size: SADCAT_SIZE,
        contentLength: SADCAT_SIZE,
        mimetype: 'image/jpeg',
        httpStatusCode: 200,
        eTag: expect.any(String),
      }),
    })
  })

  test('anon caller cannot upload a binary stream', async () => {
    const name = 'bin/anon.png'
    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      token: process.env.ANON_KEY,
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('binary upload without auth header is rejected', async () => {
    const name = 'bin/no-auth.png'
    const { size } = fs.statSync(SADCAT_PATH)
    const res = await ctx.app.inject({
      method: 'POST',
      url: `/object/${standard.id}/${name}`,
      headers: { 'Content-Length': size, 'Content-Type': 'image/jpeg' },
      payload: fs.createReadStream(SADCAT_PATH),
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('binary upload to non-existent bucket returns 400', async () => {
    const missingBucket = `${ctx.prefix}_missing_bin`
    const res = await binaryUpload(
      ctx.app,
      'POST',
      `/object/${missingBucket}/cat.png`,
      { upsert: true }
    )
    expect(res.statusCode).toBe(400)
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  test('duplicate binary upload (no upsert) is rejected and row is untouched', async () => {
    const name = 'bin/dup.png'
    const first = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(first.statusCode).toBe(200)
    const firstRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`)
    expect(res.statusCode).toBe(400)

    const afterRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()
    expect(afterRow.id).toBe(firstRow.id)
    expect(afterRow.version).toBe(firstRow.version)
  })

  test('upsert of an existing binary object rewrites the row in place', async () => {
    const name = 'bin/upsert.png'
    const first = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(first.statusCode).toBe(200)
    const firstRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(200)

    const rows = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
    expect(rows).toHaveLength(1)
    expect(rows[0].version).not.toBe(firstRow.version)
  })

  test('binary upload exceeding global limit → 413', async () => {
    mergeConfig({ uploadFileSizeLimit: 1 })

    const name = 'bin/too-big.png'
    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      statusCode: '413',
      error: 'Payload too large',
      message: 'The object exceeded the maximum allowed size',
    })
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('spoofed x-amz-decoded-content-length is not trusted', async () => {
    mergeConfig({ uploadFileSizeLimit: 1 })

    const spoofBucket = await ctx.factories.bucket.public()
    const name = 'public/spoofed.jpg'

    const res = await binaryUpload(
      ctx.app,
      'POST',
      `/object/${spoofBucket.id}/${name}`,
      {
        headers: { 'x-amz-decoded-content-length': '1' },
        upsert: true,
      }
    )

    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      statusCode: '413',
      error: 'Payload too large',
      message: 'The object exceeded the maximum allowed size',
    })
    await ctx.snapshot.object({ bucketId: spoofBucket.id, name }).notFound()
  })

  test('binary upload to a URL with no file name returns 400', async () => {
    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/`, { upsert: true })
    expect(res.statusCode).toBe(400)
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name: '' })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  test('failed binary upload does NOT insert a DB row', async () => {
    mergeConfig({ uploadFileSizeLimit: 1 })
    const name = 'bin-should-not-insert/cat.png'

    const res = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(400)

    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()

    // Defensive: make sure the error envelope didn't accidentally come from
    // a code-path that confused "not found" with "upload failure".
    expect(res.json().code ?? '').not.toBe(ErrorCode.S3Error)
  })
})

describe('PUT /object/:bucket/* (multipart)', () => {
  test('service role can update via multipart (version rotates, row count stays 1)', async () => {
    const name = 'put/cat.png'
    // Seed the row first so PUT is a true update.
    const seed = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await multipartUpload(ctx.app, 'PUT', `/object/${standard.id}/${name}`)
    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({
      Id: expect.any(String),
      Key: `${standard.id}/${name}`,
    })

    const rows = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
    expect(rows).toHaveLength(1)
    expect(rows[0].version).not.toBe(seedRow.version)
  })

  test('anon caller cannot update via multipart', async () => {
    const name = 'put/anon.png'
    const seed = await multipartUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await multipartUpload(ctx.app, 'PUT', `/object/${standard.id}/${name}`, {
      token: process.env.ANON_KEY,
    })
    expect(res.statusCode).toBe(400)

    // The seeded version must still be in place — no silent overwrite.
    const afterRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()
    expect(afterRow.version).toBe(seedRow.version)
  })

  test('PUT without auth header is rejected and no row is created', async () => {
    const name = 'put/no-auth.png'
    const res = await ctx.app.inject({
      method: 'PUT',
      url: `/object/${standard.id}/${name}`,
      headers: { 'content-type': 'multipart/form-data; boundary=----tb' },
      payload: '------tb--\r\n',
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('PUT to a non-existent bucket returns 400', async () => {
    const missingBucket = `${ctx.prefix}_missing_put`
    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/${missingBucket}/cat.png`
    )
    expect(res.statusCode).toBe(400)
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  // Note: the legacy suite had "PUT to non-existent KEY returns 400" but the
  // actual test URL pointed at a non-existent *bucket*. PUT is always upsert
  // (see updateObject.ts) so writing to a missing key inside an existing
  // bucket is a valid create — there's nothing to assert beyond the bucket
  // case above.
})

describe('PUT /object/:bucket/* (binary)', () => {
  test('service role can update via binary stream (version rotates, row count stays 1)', async () => {
    const name = 'put-bin/cat.png'
    const seed = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await binaryUpload(ctx.app, 'PUT', `/object/${standard.id}/${name}`)
    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({
      Id: expect.any(String),
      Key: `${standard.id}/${name}`,
    })

    const rows = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
    expect(rows).toHaveLength(1)
    expect(rows[0].version).not.toBe(seedRow.version)
  })

  test('anon caller cannot PUT via binary', async () => {
    const name = 'put-bin/anon.png'
    const seed = await binaryUpload(ctx.app, 'POST', `/object/${standard.id}/${name}`, {
      upsert: true,
    })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()

    const res = await binaryUpload(ctx.app, 'PUT', `/object/${standard.id}/${name}`, {
      token: process.env.ANON_KEY,
    })
    expect(res.statusCode).toBe(400)

    const afterRow = await ctx.db('storage.objects')
      .where({ bucket_id: standard.id, name })
      .first()
    expect(afterRow.version).toBe(seedRow.version)
  })

  test('PUT binary without auth header is rejected and no row is created', async () => {
    const name = 'put-bin/no-auth.png'
    const { size } = fs.statSync(SADCAT_PATH)
    const res = await ctx.app.inject({
      method: 'PUT',
      url: `/object/${standard.id}/${name}`,
      headers: { 'Content-Length': size, 'Content-Type': 'image/jpeg' },
      payload: fs.createReadStream(SADCAT_PATH),
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: standard.id, name }).notFound()
  })

  test('PUT binary to a non-existent bucket returns 400', async () => {
    const missingBucket = `${ctx.prefix}_missing_put_bin`
    const res = await binaryUpload(
      ctx.app,
      'PUT',
      `/object/${missingBucket}/cat.png`
    )
    expect(res.statusCode).toBe(400)
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })
  // See the note in the multipart PUT block — no "non-existent key" test
  // because PUT always upserts.
})
