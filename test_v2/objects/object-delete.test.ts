import { beforeAll, describe, expect, test } from 'vitest'
import { multipartUpload, useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports `testing delete object` and `testing deleting multiple objects` from
 * the legacy suite. The bulk-delete test pushes 10 001 synthetic rows directly
 * into Postgres via the factory (no S3 side-effects) so we retain parity with
 * the legacy "10 001 prefix" coverage without hammering MinIO.
 */

let bucket: TestBucket

beforeAll(async () => {
  bucket = await ctx.factories.bucket.create()
})

describe('DELETE /object/:bucket/*', () => {
  test('service role can delete an object and the row is gone', async () => {
    const name = 'delete/one.png'
    await multipartUpload(ctx.app, 'POST', `/object/${bucket.id}/${name}`, { upsert: true })

    const res = await ctx.client.asService().delete(`/object/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('anon caller cannot delete (RLS)', async () => {
    const name = 'delete/anon-denied.png'
    await multipartUpload(ctx.app, 'POST', `/object/${bucket.id}/${name}`, { upsert: true })

    const res = await ctx.client.asAnon().delete(`/object/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(400)

    // Row is still there.
    await ctx.snapshot.object({ bucketId: bucket.id, name }).matches({ name })
  })

  test('missing auth header is rejected — row untouched', async () => {
    const name = 'delete/no-auth.png'
    await multipartUpload(ctx.app, 'POST', `/object/${bucket.id}/${name}`, { upsert: true })

    const res = await ctx.client.unauthenticated().delete(`/object/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(400)

    await ctx.snapshot.object({ bucketId: bucket.id, name }).matches({ name })
  })

  test('delete from a non-existent bucket returns 400', async () => {
    const missingBucket = `${ctx.prefix}_missing_del`
    const res = await ctx.client.asService().delete(`/object/${missingBucket}/any.png`)
    expect(res.statusCode).toBe(400)
    // Defensive: no row ever existed, and the call didn't accidentally create one.
    const count = await ctx.db('storage.objects')
      .where({ bucket_id: missingBucket })
      .count('* as c')
    expect(Number(count[0].c)).toBe(0)
  })

  test('delete a non-existent key returns 400', async () => {
    const res = await ctx.client.asService().delete(`/object/${bucket.id}/never-existed.png`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot
      .object({ bucketId: bucket.id, name: 'never-existed.png' })
      .notFound()
  })
})

describe('DELETE /object/:bucket (bulk prefixes)', () => {
  test('service role can bulk delete 10 001 prefixes in a single call', async () => {
    const bulkBucket = await ctx.factories.bucket.create()
    const prefixes = Array.from({ length: 10001 }, (_, i) => `bulk/${i}.png`)

    // Direct DB insert — 10 001 rows would be painful to push through the
    // real upload pipeline. The bulk-delete route is a pure DB operation
    // after permission checks, so DB-only rows are enough coverage.
    await ctx.factories.objectsIn(bulkBucket).createMany(prefixes.length, (i) => ({
      name: prefixes[i - 1],
    }))

    const res = await ctx.client.asService().delete(`/object/${bulkBucket.id}`, {
      payload: { prefixes },
      headers: { 'content-type': 'application/json' },
    })

    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ name: string }>
    expect(body).toHaveLength(10001)
    const names = new Set(body.map((r) => r.name))
    expect(names.has('bulk/0.png')).toBe(true)
    expect(names.has('bulk/10000.png')).toBe(true)

    const leftover = await ctx.db('storage.objects').where({ bucket_id: bulkBucket.id }).count('* as c')
    expect(Number(leftover[0].c)).toBe(0)
  })

  test('anon caller gets an empty result (filtered by RLS)', async () => {
    const b = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(b).create({ name: 'bulk-anon/a.png' })

    const res = await ctx.client.asAnon().delete(`/object/${b.id}`, {
      payload: { prefixes: ['bulk-anon/a.png', 'bulk-anon/b.png'] },
      headers: { 'content-type': 'application/json' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])

    // Row untouched.
    await ctx.snapshot.object({ bucketId: b.id, name: 'bulk-anon/a.png' }).matches({
      name: 'bulk-anon/a.png',
    })
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client.unauthenticated().delete(`/object/${bucket.id}`, {
      payload: { prefixes: ['a', 'b'] },
      headers: { 'content-type': 'application/json' },
    })
    expect(res.statusCode).toBe(400)
  })

  test('bulk delete from a non-existent bucket returns an empty array', async () => {
    const res = await ctx.client.asService().delete(`/object/${ctx.prefix}_missing_bulk`, {
      payload: { prefixes: ['anything.png'] },
      headers: { 'content-type': 'application/json' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  test('bulk delete with prefixes that do not match anything returns an empty array', async () => {
    const res = await ctx.client.asService().delete(`/object/${bucket.id}`, {
      payload: { prefixes: ['nothing/here.png', 'nothing/there.png'] },
      headers: { 'content-type': 'application/json' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })
})
