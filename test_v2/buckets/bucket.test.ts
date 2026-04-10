import { describe, expect, test } from 'vitest'
import { useTestContext } from '@internal/testing/helpers'

const ctx = useTestContext()

describe('GET /bucket/:id', () => {
  test('returns bucket details for an authorized caller', async () => {
    const bucket = await ctx.factories.bucket.create()

    const res = await ctx.client.asService().get(`/bucket/${bucket.id}`)

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({
      id: bucket.id,
      name: bucket.name,
      public: false,
      file_size_limit: null,
      allowed_mime_types: null,
    })
  })

  test('anon caller is denied (RLS)', async () => {
    const bucket = await ctx.factories.bucket.create()

    const res = await ctx.client.asAnon().get(`/bucket/${bucket.id}`)

    expect(res.statusCode).toBe(400)
  })

  test('missing auth header is rejected', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.unauthenticated().get(`/bucket/${bucket.id}`)
    expect(res.statusCode).toBe(400)
  })

  test('returns 404 for a non-existent bucket', async () => {
    const res = await ctx.client.asAnon().get(`/object/${ctx.prefix}_does_not_exist`)
    expect(res.statusCode).toBe(404)
  })
})

describe('GET /bucket', () => {
  test('lists every bucket the caller can see', async () => {
    const created = await ctx.factories.bucket.createMany(3)

    const res = await ctx.client.asService().get('/bucket')

    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ id: string; name: string; public: boolean }>
    const returnedIds = new Set(body.map((b) => b.id))
    for (const c of created) {
      expect(returnedIds.has(c.id)).toBe(true)
    }
    expect(body[0]).toMatchObject({
      id: expect.any(String),
      name: expect.any(String),
      public: expect.any(Boolean),
    })
  })

  test.each([
    {
      headers: {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/138.0.0.0 Safari/537.36',
      },
      shouldIncludeType: true,
    },
    { headers: { 'user-agent': 'supabase-py/storage3 v0.11.9' }, shouldIncludeType: false },
    { headers: { 'user-agent': 'supabase-py/storage3 v0.12.0' }, shouldIncludeType: false },
    { headers: { 'user-agent': 'supabase-py/storage3 v0.12.1' }, shouldIncludeType: true },
    { headers: { 'user-agent': 'supabase-py/storage3 v1.0.0' }, shouldIncludeType: true },
    { headers: { 'x-client-info': 'supabase-py/2.17.3' }, shouldIncludeType: false },
    { headers: { 'x-client-info': 'supabase-py/2.18.0' }, shouldIncludeType: true },
    { headers: { 'x-client-info': 'supabase-py/2.19.0' }, shouldIncludeType: true },
  ])(
    'includes bucket type only for clients that understand it ($headers)',
    async ({ headers, shouldIncludeType }) => {
      await ctx.factories.bucket.create()

      const res = await ctx.client.asService().get('/bucket', { headers })

      expect(res.statusCode).toBe(200)
      const [first] = res.json() as Array<{ type?: string }>
      expect(first.type).toBe(shouldIncludeType ? 'STANDARD' : undefined)
    }
  )

  test('anon caller sees no buckets', async () => {
    await ctx.factories.bucket.create()
    const res = await ctx.client.asAnon().get('/bucket')
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client.unauthenticated().get('/bucket')
    expect(res.statusCode).toBe(400)
  })

  test('limit/offset/sort/search are honored', async () => {
    const created = await ctx.factories.bucket.createMany(4, (i) => ({
      name: `${ctx.prefix}_search_${i.toString().padStart(3, '0')}`,
    }))

    const res = await ctx.client
      .asService()
      .get(`/bucket?limit=1&offset=0&sortColumn=name&sortOrder=asc&search=${ctx.prefix}_search_`)

    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ id: string; name: string }>
    expect(body).toHaveLength(1)
    expect(body[0].id).toBe(created[0].id)
  })

  test('limit=0 → 400', async () => {
    const res = await ctx.client.asService().get('/bucket?limit=0')
    expect(res.statusCode).toBe(400)
  })

  test('offset=-1 → 400', async () => {
    const res = await ctx.client.asService().get('/bucket?offset=-1')
    expect(res.statusCode).toBe(400)
  })
})

describe('POST /bucket', () => {
  test('creates a bucket and the row matches the request', async () => {
    const name = `${ctx.prefix}_create`

    const res = await ctx.client.asService().post('/bucket', { name })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({ name })
    ctx.track.bucket(name)

    // Side-effect: row landed with the right defaults.
    await ctx.snapshot.bucket({ id: name }).matches({
      id: name,
      name,
      public: false,
      file_size_limit: null,
      allowed_mime_types: null,
      type: 'STANDARD',
    })
  })

  test('rejects bucket names containing /', async () => {
    const res = await ctx.client.asService().post('/bucket', { name: `${ctx.prefix}/sub` })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      error: 'Invalid Input',
      message: 'Bucket name invalid',
      statusCode: '400',
    })
  })

  test('anon caller cannot create a bucket', async () => {
    const name = `${ctx.prefix}_anon`
    const res = await ctx.client.asAnon().post('/bucket', { name })
    expect(res.statusCode).toBe(400)
    // Defensive: nothing landed in storage.buckets for this name.
    await ctx.snapshot.bucket({ id: name }).notFound()
  })

  test('missing auth header is rejected', async () => {
    const name = `${ctx.prefix}_no_auth`
    const res = await ctx.client.unauthenticated().post('/bucket', { name })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: name }).notFound()
  })

  test('duplicate name is rejected', async () => {
    const existing = await ctx.factories.bucket.create()
    const res = await ctx.client.asService().post('/bucket', { name: existing.name })
    expect(res.statusCode).toBe(400)
  })

  test('rejects names longer than 100 chars', async () => {
    const res = await ctx.client.asService().post('/bucket', { name: 'a'.repeat(101) })
    expect(res.statusCode).toBe(400)
  })

  test('rejects names with leading or trailing whitespace', async () => {
    const res = await ctx.client.asService().post('/bucket', { name: ` ${ctx.prefix}_ws` })
    expect(res.statusCode).toBe(400)
    const body = res.json() as { statusCode: string; error: string }
    expect(body.statusCode).toBe('400')
    expect(body.error).toBe('Invalid Input')
  })
})

describe('PUT /bucket/:id (public toggle)', () => {
  test('flips a bucket between public and private', async () => {
    const bucket = await ctx.factories.bucket.create()
    const svc = ctx.client.asService()

    const makePublic = await svc.put(`/bucket/${bucket.id}`, { public: true })
    expect(makePublic.statusCode).toBe(200)
    expect(makePublic.json()).toEqual({ message: 'Successfully updated' })
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ public: true })

    const makePrivate = await svc.put(`/bucket/${bucket.id}`, { public: false })
    expect(makePrivate.statusCode).toBe(200)
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ public: false })
  })

  test('anon caller cannot toggle visibility — row unchanged', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asAnon().put(`/bucket/${bucket.id}`, { public: true })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ public: false })
  })

  test('missing auth header is rejected — row unchanged', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.unauthenticated().put(`/bucket/${bucket.id}`, { public: true })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ public: false })
  })

  test('non-existent bucket is rejected and nothing is created', async () => {
    const name = `${ctx.prefix}_missing`
    const res = await ctx.client
      .unauthenticated()
      .put(`/bucket/${name}`, { public: true })
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: name }).notFound()
  })
})

describe('DELETE /bucket/:id', () => {
  test('deletes an empty bucket', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asService().delete(`/bucket/${bucket.id}`)
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({ message: 'Successfully deleted' })
    await ctx.snapshot.bucket({ id: bucket.id }).notFound()
  })

  test('anon caller cannot delete — bucket still present', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asAnon().delete(`/bucket/${bucket.id}`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ id: bucket.id })
  })

  test('missing auth header is rejected — bucket still present', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.unauthenticated().delete(`/bucket/${bucket.id}`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: bucket.id }).matches({ id: bucket.id })
  })

  test('non-empty bucket is refused — bucket and its object still present', async () => {
    const bucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(bucket).create({ name: 'present.png' })

    const res = await ctx.client.asService().delete(`/bucket/${bucket.id}`)
    expect(res.statusCode).toBe(400)

    await ctx.snapshot.bucket({ id: bucket.id }).matches({ id: bucket.id })
    await ctx.snapshot.object({ bucketId: bucket.id, name: 'present.png' }).matches({
      name: 'present.png',
    })
  })

  test('non-existent bucket is refused', async () => {
    const name = `${ctx.prefix}_missing`
    const res = await ctx.client.asService().delete(`/bucket/${name}`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: name }).notFound()
  })

  test('empty json body is accepted', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asService().inject({
      method: 'DELETE',
      url: `/bucket/${bucket.id}`,
      headers: { 'content-type': 'application/json' },
      payload: '',
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({ message: 'Successfully deleted' })
  })

  test('empty json body for missing bucket → 404 envelope', async () => {
    const res = await ctx.client.asService().inject({
      method: 'DELETE',
      url: `/bucket/${ctx.prefix}_missing_404`,
      headers: { 'content-type': 'application/json' },
      payload: '',
    })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      statusCode: '404',
      error: 'Bucket not found',
      message: 'Bucket not found',
    })
  })
})

describe('POST /bucket/:id/empty', () => {
  test('queues an empty job for a bucket with objects', async () => {
    const bucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(bucket).createMany(3)

    const res = await ctx.client.asService().post(`/bucket/${bucket.id}/empty`)

    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({
      message: 'Empty bucket has been queued. Completion may take up to an hour.',
    })
  })

  test('queues an empty job for an already-empty bucket', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asService().post(`/bucket/${bucket.id}/empty`)
    expect(res.statusCode).toBe(200)
  })

  test('anon caller is denied — seeded objects are untouched', async () => {
    const bucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(bucket).createMany(3, (i) => ({
      name: `fixtures/anon-${i}.png`,
    }))

    const res = await ctx.client.asAnon().post(`/bucket/${bucket.id}/empty`)
    expect(res.statusCode).toBe(400)

    const count = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id })
      .count('* as c')
    expect(Number(count[0].c)).toBe(3)
  })

  test('missing auth header is rejected — seeded objects are untouched', async () => {
    const bucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(bucket).createMany(3, (i) => ({
      name: `fixtures/no-auth-${i}.png`,
    }))

    const res = await ctx.client.unauthenticated().post(`/bucket/${bucket.id}/empty`)
    expect(res.statusCode).toBe(400)

    const count = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id })
      .count('* as c')
    expect(Number(count[0].c)).toBe(3)
  })

  test('non-existent bucket is refused', async () => {
    const name = `${ctx.prefix}_missing`
    const res = await ctx.client.asService().post(`/bucket/${name}/empty`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.bucket({ id: name }).notFound()
  })

  test('empty json body is accepted', async () => {
    const bucket = await ctx.factories.bucket.create()
    const res = await ctx.client.asService().inject({
      method: 'POST',
      url: `/bucket/${bucket.id}/empty`,
      headers: { 'content-type': 'application/json' },
      payload: '',
    })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({
      message: 'Empty bucket has been queued. Completion may take up to an hour.',
    })
  })

  test('empty json body for missing bucket → 404 envelope', async () => {
    const res = await ctx.client.asService().inject({
      method: 'POST',
      url: `/bucket/${ctx.prefix}_empty_404/empty`,
      headers: { 'content-type': 'application/json' },
      payload: '',
    })
    expect(res.statusCode).toBe(400)
    expect(res.json()).toEqual({
      statusCode: '404',
      error: 'Bucket not found',
      message: 'Bucket not found',
    })
  })
})

describe('object count helpers', () => {
  test('seeded objects land in the bucket', async () => {
    const bucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(bucket).createMany(27, (i) => ({
      name: `fixtures/count-object-${i}`,
    }))

    const total = await ctx.db('storage.objects').where({ bucket_id: bucket.id }).count('* as c')
    expect(Number(total[0].c)).toBe(27)
  })
})
