import { describe, expect, test } from 'vitest'
import { escapeLike } from '@storage/database/knex'
import { useTestContext } from '@internal/testing/helpers'

const ctx = useTestContext()

describe('search filter wildcard escaping', () => {
  test('escapeLike should escape SQL wildcard characters', () => {
    expect(escapeLike('%_abc')).toBe('\\%\\_abc')
    expect(escapeLike('a%b_c')).toBe('a\\%b\\_c')
    expect(escapeLike('plain-text')).toBe('plain-text')
  })

  test('bucket search treats % as a literal character', async () => {
    const res = await ctx.client.asService().get('/bucket?search=%25')
    expect(res.statusCode).toBe(200)
    expect(res.json<{ name: string }[]>()).toHaveLength(0)
  })

  test('bucket search treats _ as a literal character', async () => {
    const literal = await ctx.factories.bucket.create({
      name: `${ctx.prefix}_escwild_aa`,
    })
    const wildcardDecoy = await ctx.factories.bucket.create({
      name: `${ctx.prefix}_escwildXaa`,
    })

    const res = await ctx.client
      .asService()
      .get(`/bucket?search=${encodeURIComponent(literal.name)}`)

    expect(res.statusCode).toBe(200)
    const names = res.json<{ name: string }[]>().map((b) => b.name)
    expect(names).toContain(literal.name)
    expect(names).not.toContain(wildcardDecoy.name)
  })

  test('analytics bucket search treats % as a literal character', async () => {
    const res = await ctx.client.asService().get('/iceberg/bucket?search=%25')
    expect(res.statusCode).toBe(200)
    expect(res.json<{ name: string }[]>()).toHaveLength(0)
  })
})
