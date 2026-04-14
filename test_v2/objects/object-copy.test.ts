import { beforeAll, describe, expect, test } from 'vitest'
import { multipartUpload, useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports the `testing copy object` describe block. The legacy suite relied on
 * `bucket2` / `bucket3` being pre-seeded with a "casestudy.png" row whose
 * user_metadata was `{ test1: 1234 }` — we recreate that shape inside each
 * test using the factory + the real upload pipeline so the copy route has
 * actual bytes to work with on MinIO.
 */

const SOURCE_NAME = 'source/casestudy.png'
const SOURCE_USER_METADATA = { test1: 1234 }

let sourceBucket: TestBucket
let destBucket: TestBucket

beforeAll(async () => {
  sourceBucket = await ctx.factories.bucket.create()
  destBucket = await ctx.factories.bucket.create()
})

async function seedSource(): Promise<void> {
  const res = await multipartUpload(ctx.app, 'POST', `/object/${sourceBucket.id}/${SOURCE_NAME}`, {
    upsert: true,
    fields: { metadata: JSON.stringify(SOURCE_USER_METADATA) },
  })
  expect(res.statusCode).toBe(200)
}

describe('POST /object/copy', () => {
  test('service role can copy within the same bucket', async () => {
    await seedSource()
    const destKey = 'dest/casestudy-within.png'

    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationKey: destKey,
    })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({ Key: `${sourceBucket.id}/${destKey}` })

    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).matches({
      bucket_id: sourceBucket.id,
      name: destKey,
    })
  })

  test('can copy across buckets', async () => {
    await seedSource()
    const destKey = 'dest/casestudy-cross.png'

    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationBucket: destBucket.id,
      destinationKey: destKey,
    })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({ Key: `${destBucket.id}/${destKey}` })

    await ctx.snapshot.object({ bucketId: destBucket.id, name: destKey }).matches({
      bucket_id: destBucket.id,
      name: destKey,
    })
  })

  test('copyMetadata: true carries user_metadata forward', async () => {
    await seedSource()
    const destKey = 'dest/copy-keep-metadata.png'

    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationKey: destKey,
      copyMetadata: true,
    })

    expect(res.statusCode).toBe(200)
    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).matches({
      user_metadata: SOURCE_USER_METADATA,
    })
  })

  test('copying to self with x-metadata overwrites user_metadata + metadata.mimetype', async () => {
    await seedSource()
    const destKey = 'dest/copy-overwrite.png'

    // Seed the dest separately so copying-to-self has something to overwrite.
    const seed = await multipartUpload(
      ctx.app,
      'POST',
      `/object/${sourceBucket.id}/${destKey}`,
      {
        upsert: true,
        fields: { metadata: JSON.stringify(SOURCE_USER_METADATA) },
      }
    )
    expect(seed.statusCode).toBe(200)

    const res = await ctx.client.asService().post(
      '/object/copy',
      {
        bucketId: sourceBucket.id,
        sourceKey: destKey,
        destinationKey: destKey,
        metadata: {
          cacheControl: 'max-age=999',
          mimetype: 'image/gif',
        },
        copyMetadata: false,
      },
      {
        headers: {
          'x-upsert': 'true',
          'x-metadata': Buffer.from(
            JSON.stringify({ newMetadata: 'test1' })
          ).toString('base64'),
        },
      }
    )

    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body).toMatchObject({
      Key: `${sourceBucket.id}/${destKey}`,
      name: destKey,
      bucket_id: sourceBucket.id,
      metadata: expect.objectContaining({
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      }),
    })

    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).matches({
      user_metadata: { newMetadata: 'test1' },
      metadata: expect.objectContaining({
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      }),
    })
  })

  test('copyMetadata: false clears user_metadata on the destination', async () => {
    await seedSource()
    const destKey = 'dest/copy-strip-metadata.png'

    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationKey: destKey,
      copyMetadata: false,
    })

    expect(res.statusCode).toBe(200)
    const row = await ctx.db('storage.objects')
      .where({ bucket_id: sourceBucket.id, name: destKey })
      .first()
    expect(row).toBeTruthy()
    expect(row.user_metadata).toBeNull()
  })

  test('anon cannot copy (RLS) — source preserved, dest never created', async () => {
    await seedSource()
    const destKey = 'dest/anon-denied.png'

    const res = await ctx.client.asAnon().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationKey: destKey,
    })
    expect(res.statusCode).toBe(400)

    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: SOURCE_NAME }).matches({
      user_metadata: SOURCE_USER_METADATA,
    })
    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).notFound()
  })

  test('copy without auth header is rejected — source preserved, dest never created', async () => {
    await seedSource()
    const destKey = 'dest/no-auth.png'

    const res = await ctx.client.unauthenticated().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: SOURCE_NAME,
      destinationKey: destKey,
    })
    expect(res.statusCode).toBe(400)

    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: SOURCE_NAME }).matches({
      user_metadata: SOURCE_USER_METADATA,
    })
    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).notFound()
  })

  test('copy from a non-existent bucket returns 400 and persists nothing', async () => {
    const missingBucket = `${ctx.prefix}_missing_copy`
    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: missingBucket,
      sourceKey: 'x',
      destinationKey: 'y',
    })
    expect(res.statusCode).toBe(400)
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  test('copy from a non-existent key returns 400 and leaves the destination empty', async () => {
    const destKey = 'never/lands.png'
    const res = await ctx.client.asService().post('/object/copy', {
      bucketId: sourceBucket.id,
      sourceKey: 'does/not/exist.png',
      destinationKey: destKey,
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: sourceBucket.id, name: destKey }).notFound()
  })
})
