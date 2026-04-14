import { beforeAll, describe, expect, test } from 'vitest'
import { useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext()

/**
 * Ports `testing list objects` from the legacy suite. These tests focus on
 * the DB-level listing logic (prefix matching, sorting, limit/offset, LIKE
 * literals), so we seed rows directly via the factory — no S3 round-trips
 * needed. Each test gets its own bucket so the assertions stay deterministic.
 */

let listBucket: TestBucket

async function seedListing(bucket: TestBucket): Promise<void> {
  const factory = ctx.factories.objectsIn(bucket)
  await factory.create({ name: 'curlimage.jpg' })
  await factory.create({ name: 'folder/only_uid.jpg' })
  await factory.create({ name: 'folder/subfolder/public-all-permissions.png' })
  await factory.create({ name: 'folder/UPPER-folder/public-all-permissions.png' })
  await factory.create({ name: 'public/sadcat-upload.png' })
  await factory.create({ name: 'public/sadcat-upload23.png' })
  await factory.create({ name: 'private/sadcat-upload3.png' })
  await factory.create({ name: 'authenticated/a.png' })
  await factory.create({ name: 'authenticated/b.png' })
}

beforeAll(async () => {
  listBucket = await ctx.factories.bucket.create()
  await seedListing(listBucket)
})

describe('POST /object/list/:bucket', () => {
  test('listing the root returns top-level entries (files + folders)', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: '',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    const names = (res.json() as Array<{ name: string }>).map((e) => e.name)
    expect(names).toContain('curlimage.jpg')
    expect(names).toContain('folder')
    expect(names).toContain('public')
    expect(names).toContain('private')
    expect(names).toContain('authenticated')
  })

  test('listing inside a folder returns the folder contents', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: 'folder',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    const names = (res.json() as Array<{ name: string }>).map((e) => e.name)
    expect(names).toContain('only_uid.jpg')
    expect(names).toContain('subfolder')
    expect(names).toContain('UPPER-folder')
  })

  test('listing a non-existent prefix returns []', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: 'notfound',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  test('limit is honored', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: '',
      limit: 2,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toHaveLength(2)
  })

  test('anon caller sees [] (no matching RLS policies)', async () => {
    const res = await ctx.client.asAnon().post(`/object/list/${listBucket.id}`, {
      prefix: '',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client.unauthenticated().post(`/object/list/${listBucket.id}`, {
      prefix: '',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(400)
  })

  test('prefix matching is case-insensitive', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: 'PUBLIC/',
      limit: 20,
      offset: 0,
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toHaveLength(2)
  })

  test('ascending sort by name uses byte order', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: 'public/',
      sortBy: { column: 'name', order: 'asc' },
    })
    expect(res.statusCode).toBe(200)
    const names = (res.json() as Array<{ name: string }>).map((e) => e.name)
    // Byte order (COLLATE "C"): '.' (46) < '2' (50)
    expect(names).toEqual(['sadcat-upload.png', 'sadcat-upload23.png'])
  })

  test('descending sort by name uses byte order', async () => {
    const res = await ctx.client.asService().post(`/object/list/${listBucket.id}`, {
      prefix: 'public/',
      sortBy: { column: 'name', order: 'desc' },
    })
    expect(res.statusCode).toBe(200)
    const names = (res.json() as Array<{ name: string }>).map((e) => e.name)
    expect(names).toEqual(['sadcat-upload23.png', 'sadcat-upload.png'])
  })

  test('list-v1 treats % as a literal character when sorting by non-name column', async () => {
    const percentBucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(percentBucket).createMany(2, (i) => ({
      name: `percent/${i}.txt`,
    }))

    // Searching for '%' as a prefix must NOT expand to "match everything".
    const res = await ctx.client.asService().post(`/object/list/${percentBucket.id}`, {
      prefix: '%',
      limit: 100,
      offset: 0,
      sortBy: { column: 'created_at', order: 'asc' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  test('list-v1 treats _ as a literal character when sorting by non-name column', async () => {
    const literalBucket = await ctx.factories.bucket.create()
    const literalMatch = `wild_${ctx.prefix}/hit.txt`
    const wildcardOnlyMatch = `wildX${ctx.prefix}/miss.txt`

    await ctx.factories.objectsIn(literalBucket).create({ name: literalMatch })
    await ctx.factories.objectsIn(literalBucket).create({ name: wildcardOnlyMatch })

    const res = await ctx.client.asService().post(`/object/list/${literalBucket.id}`, {
      prefix: `wild_${ctx.prefix}/`,
      limit: 100,
      offset: 0,
      sortBy: { column: 'created_at', order: 'asc' },
    })
    expect(res.statusCode).toBe(200)
    const names = (res.json() as Array<{ name: string }>).map((e) => e.name)
    expect(names).toEqual(['hit.txt'])
  })
})
