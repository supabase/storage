import { beforeAll, describe, expect, test, vi } from 'vitest'
import { multipartUpload, useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports `testing move object`. A "move" exercises copy + delete with a
 * rollback hatch, so we make sure real bytes land on MinIO first, then
 * assert that the source row disappears and the destination row shows up.
 */

let source: TestBucket
let dest: TestBucket

beforeAll(async () => {
  source = await ctx.factories.bucket.create()
  dest = await ctx.factories.bucket.create()
})

async function seed(name: string): Promise<void> {
  const res = await multipartUpload(ctx.app, 'POST', `/object/${source.id}/${name}`, {
    upsert: true,
  })
  expect(res.statusCode).toBe(200)
}

describe('POST /object/move', () => {
  test('service role can move within the same bucket', async () => {
    const sourceKey = 'move/orig-within.png'
    const destinationKey = 'move/new-within.png'
    await seed(sourceKey)

    const res = await ctx.client.asService().post('/object/move', {
      bucketId: source.id,
      sourceKey,
      destinationKey,
    })

    expect(res.statusCode).toBe(200)
    await ctx.snapshot.object({ bucketId: source.id, name: sourceKey }).notFound()
    await ctx.snapshot.object({ bucketId: source.id, name: destinationKey }).matches({
      name: destinationKey,
    })
  })

  test('service role can move across buckets', async () => {
    const sourceKey = 'move/orig-cross.png'
    const destinationKey = 'move/new-cross.png'
    await seed(sourceKey)

    const res = await ctx.client.asService().post('/object/move', {
      bucketId: source.id,
      sourceKey,
      destinationBucket: dest.id,
      destinationKey,
    })

    expect(res.statusCode).toBe(200)
    await ctx.snapshot.object({ bucketId: source.id, name: sourceKey }).notFound()
    await ctx.snapshot.object({ bucketId: dest.id, name: destinationKey }).matches({
      name: destinationKey,
      bucket_id: dest.id,
    })
  })

  test('cross-bucket move rollback cleans up the destination when S3 fails', async () => {
    const sourceKey = 'move/rollback-source.png'
    const destinationKey = 'move/rollback-dest.png'
    await seed(sourceKey)

    // Force headObject (which move uses after copy) to fail so the rollback
    // path fires. We dynamic-import backends inside the test so the spy is
    // wired after the fresh fastify instance built in beforeEach.
    const { backends } = await import('../../src/storage')
    const headSpy = vi
      .spyOn(backends.S3Backend.prototype, 'headObject')
      .mockRejectedValueOnce(new Error('forced move failure'))

    try {
      const res = await ctx.client.asService().post('/object/move', {
        bucketId: source.id,
        sourceKey,
        destinationBucket: dest.id,
        destinationKey,
      })

      expect(res.statusCode).toBeGreaterThanOrEqual(400)
    } finally {
      headSpy.mockRestore()
    }

    // The source row is still where it started; dest row did not stick.
    await ctx.snapshot.object({ bucketId: source.id, name: sourceKey }).matches({
      name: sourceKey,
    })
    await ctx.snapshot.object({ bucketId: dest.id, name: destinationKey }).notFound()
  })

  test('anon caller cannot move (RLS)', async () => {
    const sourceKey = 'move/anon-source.png'
    await seed(sourceKey)

    const res = await ctx.client.asAnon().post('/object/move', {
      bucketId: source.id,
      sourceKey,
      destinationKey: 'move/anon-dest.png',
    })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: source.id, name: sourceKey }).matches({
      name: sourceKey,
    })
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client.unauthenticated().post('/object/move', {
      bucketId: source.id,
      sourceKey: 'move/no-auth.png',
      destinationKey: 'move/no-auth-dest.png',
    })
    expect(res.statusCode).toBe(400)
  })

  test('move in a non-existent bucket returns 400', async () => {
    const res = await ctx.client.asService().post('/object/move', {
      bucketId: `${ctx.prefix}_missing_move`,
      sourceKey: 'a.png',
      destinationKey: 'b.png',
    })
    expect(res.statusCode).toBe(400)
  })

  test('moving a non-existent source returns 400', async () => {
    const res = await ctx.client.asService().post('/object/move', {
      bucketId: source.id,
      sourceKey: 'move/nope.png',
      destinationKey: 'move/also-nope.png',
    })
    expect(res.statusCode).toBe(400)
  })

  test('moving to an existing destination key returns 400', async () => {
    const sourceKey = 'move/orig-collision.png'
    const destinationKey = 'move/taken-dest.png'
    await seed(sourceKey)
    await seed(destinationKey)

    const res = await ctx.client.asService().post('/object/move', {
      bucketId: source.id,
      sourceKey,
      destinationKey,
    })
    expect(res.statusCode).toBe(400)
  })
})
