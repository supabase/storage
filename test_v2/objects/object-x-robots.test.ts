import { beforeAll, describe, expect, test } from 'vitest'
import { useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports the `x-robots-tag header` describe block. Real upload + read flow so
 * the header is actually round-tripped through the storage backend rather
 * than being asserted on mocked metadata.
 */

let bucket: TestBucket

beforeAll(async () => {
  bucket = await ctx.factories.bucket.create()
})

async function createObject(
  name: string,
  headers: Record<string, string> = {}
): Promise<void> {
  const res = await ctx.client.asService().post(
    `/object/${bucket.id}/${name}`,
    new File(['test'], 'file.txt'),
    { headers }
  )
  expect(res.statusCode).toBe(200)
}

async function fetchRobotsTag(name: string): Promise<string | string[] | undefined> {
  const res = await ctx.client.asService().get(`/object/authenticated/${bucket.id}/${name}`)
  expect(res.statusCode).toBe(200)
  return res.headers['x-robots-tag']
}

describe('x-robots-tag header', () => {
  test('defaults to "none" when not specified at upload time', async () => {
    const name = 'robots/default.txt'
    await createObject(name)
    expect(await fetchRobotsTag(name)).toBe('none')
  })

  test('honors an explicit x-robots-tag on upload', async () => {
    const name = 'robots/explicit.txt'
    await createObject(name, { 'x-robots-tag': 'all' })
    expect(await fetchRobotsTag(name)).toBe('all')
  })

  test('accepts a complex robots directive on upload', async () => {
    const name = 'robots/complex.txt'
    await createObject(name, { 'x-robots-tag': 'max-snippet: 10, notranslate' })
    expect(await fetchRobotsTag(name)).toBe('max-snippet: 10, notranslate')
  })

  test('updates the header on upsert', async () => {
    const name = 'robots/updated.txt'
    await createObject(name, { 'x-robots-tag': 'all' })
    expect(await fetchRobotsTag(name)).toBe('all')

    // Upsert with a new header.
    await createObject(name, { 'x-robots-tag': 'nofollow', 'x-upsert': 'true' })
    expect(await fetchRobotsTag(name)).toBe('nofollow')
  })

  test('rejects an invalid robots directive', async () => {
    const res = await ctx.client.asService().post(
      `/object/${bucket.id}/robots/invalid.txt`,
      new File(['test'], 'file.txt'),
      { headers: { 'x-robots-tag': 'invalidrule' } }
    )
    expect(res.statusCode).toBe(400)
    expect(res.json()).toMatchObject({
      statusCode: '400',
      error: 'invalid_x_robots_tag',
      message: expect.stringContaining('invalidrule'),
    })
  })
})
