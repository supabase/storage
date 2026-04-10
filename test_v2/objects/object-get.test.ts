import { beforeAll, describe, expect, test } from 'vitest'
import { SADCAT_SIZE, binaryUpload, multipartUpload, useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports `testing GET object` from the legacy jest suite. Unlike the legacy
 * tests, we no longer rely on pre-seeded `bucket2` / `public-bucket-2` rows
 * or any `useMockObject()` plumbing — each test seeds its own bucket with the
 * factory and pushes a real object through the upload pipeline, so assertions
 * run against the real S3/MinIO backend.
 */
describe('GET /object/:bucket/*', () => {
  let privateBucket: TestBucket
  let publicBucket: TestBucket
  const objectKey = 'authenticated/sadcat.png'

  // Buckets are created once; the app/fastify instance is per-test so the
  // actual seed upload happens inside each test via `seedPrivateObject()`
  // below (still cheap — one HTTP round-trip against MinIO).
  beforeAll(async () => {
    privateBucket = await ctx.factories.bucket.create()
    publicBucket = await ctx.factories.bucket.public()
  })

  async function seedPrivateObject() {
    const res = await multipartUpload(ctx.app, 'POST', `/object/${privateBucket.id}/${objectKey}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(200)
  }

  async function seedPublicObject(name = 'favicon.ico') {
    const res = await multipartUpload(ctx.app, 'POST', `/object/${publicBucket.id}/${name}`, {
      upsert: true,
    })
    expect(res.statusCode).toBe(200)
    return name
  }

  test('service role can read a private object (happy path)', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asService().get(`/object/authenticated/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['x-robots-tag']).toBe('none')
    expect(res.headers['content-length']).toBe(String(SADCAT_SIZE))
    // The real S3 backend sets a content-type on GET.
    expect(res.headers['content-type']).toBeTruthy()
  })

  test('reading without the /authenticated prefix also works for service role', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asService().get(`/object/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-length']).toBe(String(SADCAT_SIZE))
  })

  test('forwards 304 when If-None-Match matches the stored etag', async () => {
    await seedPrivateObject()

    // First GET: capture the real etag returned by MinIO.
    const first = await ctx.client.asService().get(`/object/${privateBucket.id}/${objectKey}`)
    expect(first.statusCode).toBe(200)
    const etag = first.headers['etag']
    expect(etag).toBeTruthy()

    // Second GET with If-None-Match: should be 304.
    const notModified = await ctx.client
      .asService()
      .get(`/object/${privateBucket.id}/${objectKey}`, {
        headers: { 'if-none-match': String(etag) },
      })
    expect(notModified.statusCode).toBe(304)
  })

  test('HEAD returns metadata headers', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asService().head(`/object/authenticated/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-length']).toBe(String(SADCAT_SIZE))
    expect(res.headers['etag']).toBeTruthy()
  })

  test('HEAD without the /authenticated prefix also works', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asService().head(`/object/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-length']).toBe(String(SADCAT_SIZE))
  })

  test('HEAD on a private bucket without a JWT is rejected', async () => {
    await seedPrivateObject()

    const res = await ctx.client.unauthenticated().head(`/object/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(400)
  })

  test('public bucket HEAD works without any auth', async () => {
    const name = await seedPublicObject()

    const res = await ctx.client.unauthenticated().head(`/object/${publicBucket.id}/${name}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['etag']).toBeTruthy()
  })

  test('public bucket GET works without the /public prefix and without auth', async () => {
    const name = await seedPublicObject()

    const res = await ctx.client.unauthenticated().get(`/object/${publicBucket.id}/${name}`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-length']).toBe(String(SADCAT_SIZE))
  })

  test('?download sets content-disposition to attachment', async () => {
    await seedPrivateObject()

    const res = await ctx.client
      .asService()
      .get(`/object/authenticated/${privateBucket.id}/${objectKey}?download`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-disposition']).toBe('attachment;')
  })

  test('?download=name.ext sets content-disposition filename', async () => {
    await seedPrivateObject()

    const res = await ctx.client
      .asService()
      .get(`/object/authenticated/${privateBucket.id}/${objectKey}?download=testname.png`)

    expect(res.statusCode).toBe(200)
    expect(res.headers['content-disposition']).toBe(
      `attachment; filename=testname.png; filename*=UTF-8''testname.png`
    )
  })

  test('anon caller cannot read a private object', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asAnon().get(`/object/authenticated/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(400)
  })

  test('anon caller cannot read a private object without the /authenticated prefix either', async () => {
    await seedPrivateObject()

    const res = await ctx.client.asAnon().get(`/object/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(400)
  })

  test('missing auth header is rejected on private bucket', async () => {
    await seedPrivateObject()

    const res = await ctx.client.unauthenticated().get(`/object/authenticated/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(400)
  })

  test('missing auth header is rejected without /authenticated prefix too', async () => {
    await seedPrivateObject()

    const res = await ctx.client.unauthenticated().get(`/object/${privateBucket.id}/${objectKey}`)

    expect(res.statusCode).toBe(400)
  })

  test('reading a non-existent object returns 400', async () => {
    const res = await ctx.client
      .asService()
      .get(`/object/authenticated/${privateBucket.id}/authenticated/does-not-exist.png`)

    expect(res.statusCode).toBe(400)
  })

  test('reading from a non-existent bucket returns 400', async () => {
    const res = await ctx.client
      .asService()
      .get(`/object/authenticated/${ctx.prefix}_missing_bucket/authenticated/whatever.png`)

    expect(res.statusCode).toBe(400)
  })

  // Keeps the binary-upload happy-path covered at the GET layer too — the
  // legacy suite had this baked into the POST/PUT tests, but it is far
  // clearer to also assert the read side works for stream uploads.
  test('can read back an object uploaded via binary stream', async () => {
    const res = await binaryUpload(
      ctx.app,
      'POST',
      `/object/${privateBucket.id}/binary-read/${objectKey}`,
      { upsert: true }
    )
    expect(res.statusCode).toBe(200)

    const get = await ctx.client
      .asService()
      .get(`/object/${privateBucket.id}/binary-read/${objectKey}`)
    expect(get.statusCode).toBe(200)
    expect(get.headers['content-length']).toBe(String(SADCAT_SIZE))
  })
})
