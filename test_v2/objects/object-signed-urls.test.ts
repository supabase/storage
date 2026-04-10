import { beforeAll, describe, expect, test } from 'vitest'
import {
  generateHS512JWK,
  signJWT,
  verifyJWT,
  type SignedToken,
} from '@internal/auth'
import {
  getConfig,
  mergeConfig,
  type JwksConfig,
  type JwksConfigKeyOCT,
} from '../../src/config'
import { multipartUpload, useTestContext } from '@internal/testing/helpers'
import type { TestBucket } from '@internal/testing/helpers'

const ctx = useTestContext({ s3: true })

/**
 * Ports the four signed-URL describe blocks from the legacy suite:
 *
 *   - POST /object/sign/:bucket/*    (generate single)
 *   - POST /object/sign/:bucket       (generate many)
 *   - GET  /object/sign/:bucket/*    (retrieve via token)
 *   - POST /object/upload/sign/:bucket/* + PUT with token
 *
 * Every test seeds its own object through the real upload pipeline so we
 * verify the token flow against actual S3 state.
 */

let bucket: TestBucket
const OBJECT_NAME = 'signed/cat.png'

beforeAll(async () => {
  bucket = await ctx.factories.bucket.create()
})

async function seedObject(name = OBJECT_NAME): Promise<void> {
  const res = await multipartUpload(ctx.app, 'POST', `/object/${bucket.id}/${name}`, {
    upsert: true,
  })
  expect(res.statusCode).toBe(200)
}

describe('POST /object/sign/:bucket/* (single)', () => {
  test('service role can mint a signed url', async () => {
    await seedObject()

    const res = await ctx.client
      .asService()
      .post(`/object/sign/${bucket.id}/${OBJECT_NAME}`, { expiresIn: 1000 })

    expect(res.statusCode).toBe(200)
    const body = res.json() as { signedURL: string }
    expect(body.signedURL).toContain('?token=')

    const token = body.signedURL.split('?token=').pop() as string
    const { jwtSecret } = getConfig()
    const payload = (await verifyJWT(token, jwtSecret)) as SignedToken
    expect(payload.url).toBe(`${bucket.id}/${OBJECT_NAME}`)
  })

  test('urlSigningKey (jwk) overrides jwtSecret when present', async () => {
    await seedObject()

    const signingJwk = {
      ...(await generateHS512JWK()),
      kid: `test-${ctx.prefix}`,
    } as JwksConfigKeyOCT
    const jwtJWKS: JwksConfig = { keys: [signingJwk], urlSigningKey: signingJwk }
    mergeConfig({ jwtJWKS })

    const res = await ctx.client
      .asService()
      .post(`/object/sign/${bucket.id}/${OBJECT_NAME}`, { expiresIn: 1000 })

    expect(res.statusCode).toBe(200)
    const token = (res.json() as { signedURL: string }).signedURL.split('?token=').pop() as string

    // The token must verify against the jwk, not the jwtSecret.
    const payload = (await verifyJWT(token, 'wrong-secret', jwtJWKS)) as SignedToken
    expect(payload.url).toBe(`${bucket.id}/${OBJECT_NAME}`)
  })

  test('anon caller cannot mint a signed url for a private bucket', async () => {
    await seedObject()

    const res = await ctx.client
      .asAnon()
      .post(`/object/sign/${bucket.id}/${OBJECT_NAME}`, { expiresIn: 1000 })
    expect(res.statusCode).toBe(400)
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client
      .unauthenticated()
      .post(`/object/sign/${bucket.id}/${OBJECT_NAME}`, { expiresIn: 1000 })
    expect(res.statusCode).toBe(400)
  })

  test('signing on a non-existent bucket returns 400', async () => {
    const res = await ctx.client
      .asService()
      .post(`/object/sign/${ctx.prefix}_missing_sign/any.png`, { expiresIn: 1000 })
    expect(res.statusCode).toBe(400)
  })

  test('signing a non-existent key returns 400', async () => {
    const res = await ctx.client
      .asService()
      .post(`/object/sign/${bucket.id}/never-existed.png`, { expiresIn: 1000 })
    expect(res.statusCode).toBe(400)
  })
})

describe('POST /object/upload/sign/:bucket/*', () => {
  test('service role can mint an upload-sign token for a non-existent key', async () => {
    const name = `upload-sign/fresh-${ctx.prefix}.png`
    const res = await ctx.client.asService().post(`/object/upload/sign/${bucket.id}/${name}`)

    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({ url: expect.any(String) })

    // The mint call must NOT create a row.
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('anon caller cannot mint an upload-sign token and no row is created', async () => {
    const name = 'anon-denied.png'
    const res = await ctx.client
      .asAnon()
      .post(`/object/upload/sign/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('missing auth header is rejected and no row is created', async () => {
    const name = 'whatever.png'
    const res = await ctx.client
      .unauthenticated()
      .post(`/object/upload/sign/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('minting on a non-existent bucket returns 400', async () => {
    const res = await ctx.client
      .asService()
      .post(`/object/upload/sign/${ctx.prefix}_missing_upload_sign/any.png`)
    expect(res.statusCode).toBe(400)
  })

  test('minting on an existing key returns 409', async () => {
    const name = `upload-sign/existing-${ctx.prefix}.png`
    await multipartUpload(ctx.app, 'POST', `/object/${bucket.id}/${name}`, { upsert: true })

    const res = await ctx.client.asService().post(`/object/upload/sign/${bucket.id}/${name}`)
    expect(res.statusCode).toBe(400)
    expect((res.json() as { statusCode: string }).statusCode).toBe('409')
  })
})

describe('PUT /object/upload/sign/:bucket/* (consume token)', () => {
  test('upload with a valid token succeeds and persists the owner + metadata', async () => {
    const { jwtSecret } = getConfig()
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    const name = `token-upload/valid-${ctx.prefix}.png`
    const url = `${bucket.id}/${name}`
    const token = await signJWT({ owner, url }, jwtSecret, 100)

    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${url}?token=${token}`,
      { token: 'unused-bearer' }
    )

    expect(res.statusCode).toBe(200)

    await ctx.snapshot.object({ bucketId: bucket.id, name }).matches({
      bucket_id: bucket.id,
      name,
      owner,
      metadata: expect.objectContaining({
        mimetype: expect.any(String),
        size: expect.any(Number),
      }),
    })
  })

  test('upload without a token is rejected and no row is created', async () => {
    const name = 'token-missing.png'
    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${bucket.id}/${name}`,
      { token: 'unused-bearer' }
    )
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('upload with a malformed JWT is rejected and no row is created', async () => {
    const name = 'token-bad.png'
    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${bucket.id}/${name}?token=xxx`,
      { token: 'unused-bearer' }
    )
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('upload with an expired JWT is rejected and no row is created', async () => {
    const { jwtSecret } = getConfig()
    const name = 'token-expired.png'
    const url = `${bucket.id}/${name}`
    const token = await signJWT(
      { owner: '317eadce-631a-4429-a0bb-f19a7a517b4a', url },
      jwtSecret,
      -1
    )

    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${url}?token=${token}`,
      { token: 'unused-bearer' }
    )
    expect(res.statusCode).toBe(400)
    await ctx.snapshot.object({ bucketId: bucket.id, name }).notFound()
  })

  test('x-upsert on generation allows overwriting an existing file in place', async () => {
    const name = `token-upload/upsert-${ctx.prefix}.png`
    const urlToSign = `${bucket.id}/${name}`

    // 1. Seed the existing object.
    const seed = await multipartUpload(ctx.app, 'POST', `/object/${urlToSign}`, { upsert: true })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id, name })
      .first()

    // 2. Generate an upsert signed upload url.
    const signed = await ctx.client
      .asService()
      .post(`/object/upload/sign/${urlToSign}`, undefined, {
        headers: { 'x-upsert': 'true' },
      })
    expect(signed.statusCode).toBe(200)
    const token = (signed.json() as { token: string }).token

    // 3. PUT with the token — should succeed (overwrite).
    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${urlToSign}?token=${token}`,
      { token: 'unused-bearer' }
    )
    expect(res.statusCode).toBe(200)

    // Exactly one row for that key — version should be fresh.
    const rows = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id, name })
    expect(rows).toHaveLength(1)
    expect(rows[0].version).not.toBe(seedRow.version)
  })

  test('without x-upsert on generation, overwriting an existing file is rejected', async () => {
    const { jwtSecret } = getConfig()
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    const name = `token-upload/no-upsert-${ctx.prefix}.png`
    const urlToSign = `${bucket.id}/${name}`

    // Seed an existing object.
    const seed = await multipartUpload(ctx.app, 'POST', `/object/${urlToSign}`, { upsert: true })
    expect(seed.statusCode).toBe(200)
    const seedRow = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id, name })
      .first()

    // A hand-signed token without upsert intent.
    const token = await signJWT({ owner, url: urlToSign }, jwtSecret, 100)

    const res = await multipartUpload(
      ctx.app,
      'PUT',
      `/object/upload/sign/${urlToSign}?token=${token}`,
      { token: 'unused-bearer' }
    )
    expect(res.statusCode).toBe(400)

    // Existing row must be untouched (version did not rotate).
    const afterRow = await ctx.db('storage.objects')
      .where({ bucket_id: bucket.id, name })
      .first()
    expect(afterRow.version).toBe(seedRow.version)
  })
})

describe('POST /object/sign/:bucket (bulk)', () => {
  test('service role can mint 10001 urls in a single call', async () => {
    const bulkBucket = await ctx.factories.bucket.create()
    const names = Array.from({ length: 10001 }, (_, i) => `bulk-sign/${i}.png`)

    await ctx.factories.objectsIn(bulkBucket).createMany(names.length, (i) => ({
      name: names[i - 1],
    }))

    const res = await ctx.client
      .asService()
      .post(`/object/sign/${bulkBucket.id}`, { expiresIn: 1000, paths: names })

    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ path: string; signedURL?: string; error?: string }>
    expect(body).toHaveLength(10001)
  })

  test('anon caller gets per-path error entries', async () => {
    const anotherBucket = await ctx.factories.bucket.create()
    await ctx.factories.objectsIn(anotherBucket).create({ name: 'bulk-sign-anon/a.png' })

    const res = await ctx.client.asAnon().post(`/object/sign/${anotherBucket.id}`, {
      expiresIn: 1000,
      paths: ['bulk-sign-anon/a.png', 'bulk-sign-anon/missing.png'],
    })
    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ error?: string }>
    expect(body[0].error).toBe('Either the object does not exist or you do not have access to it')
    expect(body[1].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('missing auth header is rejected', async () => {
    const res = await ctx.client.unauthenticated().post(`/object/sign/${bucket.id}`, {
      expiresIn: 1000,
      paths: ['whatever.png'],
    })
    expect(res.statusCode).toBe(400)
  })

  test('non-existent bucket returns per-path errors', async () => {
    const res = await ctx.client
      .asService()
      .post(`/object/sign/${ctx.prefix}_missing_bulk_sign`, {
        expiresIn: 1000,
        paths: ['a.png', 'b.png'],
      })
    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ error?: string }>
    expect(body[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('signing a non-existent key returns a per-path error', async () => {
    const res = await ctx.client.asService().post(`/object/sign/${bucket.id}`, {
      expiresIn: 1000,
      paths: ['does/not/exist.png'],
    })
    expect(res.statusCode).toBe(200)
    const body = res.json() as Array<{ error?: string }>
    expect(body[0].error).toBe('Either the object does not exist or you do not have access to it')
  })
})

describe('GET /object/sign/:bucket/* (consume)', () => {
  test('valid token fetches the object', async () => {
    await seedObject()
    const { jwtSecret } = getConfig()
    const url = `${bucket.id}/${OBJECT_NAME}`
    const token = await signJWT({ url }, jwtSecret, 100)

    const res = await ctx.client.unauthenticated().get(`/object/sign/${url}?token=${token}`)
    expect(res.statusCode).toBe(200)
    expect(res.headers['etag']).toBeTruthy()
  })

  test('jwk-signed token is accepted', async () => {
    await seedObject()
    const signingJwk = {
      ...(await generateHS512JWK()),
      kid: `get-${ctx.prefix}`,
    } as JwksConfigKeyOCT
    mergeConfig({ jwtJWKS: { keys: [signingJwk] } })

    const url = `${bucket.id}/${OBJECT_NAME}`
    const token = await signJWT({ url }, signingJwk, 100)

    const res = await ctx.client.unauthenticated().get(`/object/sign/${url}?token=${token}`)
    expect(res.statusCode).toBe(200)
  })

  test('forwards 304 with If-None-Match', async () => {
    await seedObject()
    const { jwtSecret } = getConfig()
    const url = `${bucket.id}/${OBJECT_NAME}`
    const token = await signJWT({ url }, jwtSecret, 100)

    const first = await ctx.client.unauthenticated().get(`/object/sign/${url}?token=${token}`)
    expect(first.statusCode).toBe(200)
    const etag = first.headers['etag'] as string

    const notModified = await ctx.client
      .unauthenticated()
      .get(`/object/sign/${url}?token=${token}`, {
        headers: { 'if-none-match': etag },
      })
    expect(notModified.statusCode).toBe(304)
  })

  test('token for a different url is rejected', async () => {
    await seedObject()
    const { jwtSecret } = getConfig()
    const token = await signJWT({ url: 'wrong/path.png' }, jwtSecret, 100)

    const res = await ctx.client
      .unauthenticated()
      .get(`/object/sign/${bucket.id}/${OBJECT_NAME}?token=${token}`)
    expect(res.statusCode).toBe(400)
    const body = res.json() as { error: string }
    expect(body.error).toBe('InvalidSignature')
  })

  test('missing token is rejected', async () => {
    const res = await ctx.client.unauthenticated().get(`/object/sign/${bucket.id}/${OBJECT_NAME}`)
    expect(res.statusCode).toBe(400)
  })

  test('malformed token is rejected', async () => {
    const res = await ctx.client
      .unauthenticated()
      .get(`/object/sign/${bucket.id}/${OBJECT_NAME}?token=xxx`)
    expect(res.statusCode).toBe(400)
  })

  test('expired token is rejected', async () => {
    const { jwtSecret } = getConfig()
    const token = await signJWT({ url: `${bucket.id}/${OBJECT_NAME}` }, jwtSecret, -1)

    const res = await ctx.client
      .unauthenticated()
      .get(`/object/sign/${bucket.id}/${OBJECT_NAME}?token=${token}`)
    expect(res.statusCode).toBe(400)
  })
})
