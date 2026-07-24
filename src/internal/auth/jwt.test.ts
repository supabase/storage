import { JWT_CACHE_NAME } from '@internal/cache'
import { ErrorCode } from '@internal/errors'
import * as metrics from '@internal/monitoring/metrics'
import * as crypto from 'crypto'
import { SignJWT } from 'jose'
import { vi } from 'vitest'
import { JwksConfigKey } from '../../config'
import {
  assertValidNumericJWTExpiration,
  generateES256JWK,
  generateHS512JWK,
  getMaxNumericJWTExpiration,
  isDownloadScopedToken,
  isUploadScopedToken,
  SIGNED_URL_SCOPE_DOWNLOAD,
  SIGNED_URL_SCOPE_UPLOAD,
  signJWT,
  toPublicJwk,
  verifyJWT,
  verifyJWTWithCache,
} from './jwt'

type TestPublicKey = {
  export: () => JwksConfigKey | Record<string, string>
}

type AsymmetricKeyFixture = {
  alg: 'RS256' | 'ES256' | 'EdDSA'
  kid: string
  publicKey: TestPublicKey
  privateKey: crypto.KeyObject
}

type HmacKeyFixture = {
  alg: 'HS256'
  kid?: string
  publicKey: TestPublicKey
  privateKey: Buffer
}

type KeyFixture = AsymmetricKeyFixture | HmacKeyFixture
type AsymmetricKeyType = 'rsa' | 'ec' | 'ed25519'
type GeneratedKeyPair = {
  publicKey: crypto.KeyObject
  privateKey: crypto.KeyObject
}

const asymmetricKeyPairFactories: Record<AsymmetricKeyType, () => GeneratedKeyPair> = {
  rsa: () => crypto.generateKeyPairSync('rsa', { modulusLength: 2048 }),
  ec: () => crypto.generateKeyPairSync('ec', { namedCurve: 'P-256' }),
  ed25519: () => crypto.generateKeyPairSync('ed25519'),
}

function createAsymmetricKeyFixture(
  type: AsymmetricKeyType,
  alg: AsymmetricKeyFixture['alg'],
  kid: string
): AsymmetricKeyFixture {
  const { publicKey, privateKey } = asymmetricKeyPairFactories[type]()

  return {
    alg,
    kid,
    publicKey: {
      export: () => publicKey.export({ format: 'jwk' }) as JwksConfigKey,
    },
    privateKey,
  }
}

describe('JWT', () => {
  describe('verifyJWT with JWKS', () => {
    afterEach(() => {
      vi.restoreAllMocks()
      vi.useRealTimers()
    })

    const keys: KeyFixture[] = [
      createAsymmetricKeyFixture('rsa', 'RS256', '0'),
      createAsymmetricKeyFixture('ec', 'ES256', '1'),
      createAsymmetricKeyFixture('ed25519', 'EdDSA', '2'),
    ]

    const hmacPrivateKeyWithoutKid = crypto.randomBytes(256 / 8).toString('hex')

    // without kid, so the value from the secret argument will be taken
    keys.push({
      alg: 'HS256',
      privateKey: Buffer.from(hmacPrivateKeyWithoutKid, 'utf-8'),
      publicKey: {
        export: () => ({
          doesntmatter: 'wontbeused',
        }),
      },
    })

    const hmacPrivateKeyWithKid = crypto.randomBytes(256 / 8).toString('hex')

    // with kid, so the value from the JWKS will be used
    keys.push({
      alg: 'HS256',
      kid: keys.length.toString(),
      privateKey: Buffer.from(hmacPrivateKeyWithKid, 'utf-8'),
      publicKey: {
        export: () => ({
          kty: 'oct',
          k: Buffer.from(hmacPrivateKeyWithKid, 'utf-8').toString('base64url'),
        }),
      },
    })

    const jwks = {
      keys: keys.map(
        ({ publicKey, kid, alg }) =>
          ({
            ...publicKey.export(),
            kid,
            alg,
          }) as JwksConfigKey
      ),
    }

    keys.forEach(({ privateKey, alg, kid }, keyIdx) => {
      const iat = Math.trunc(Date.now() / 1000)
      const exp = iat + 60

      const parts = [
        Buffer.from(JSON.stringify({ typ: 'JWT', kid, alg }), 'utf-8').toString('base64url'),
        Buffer.from(JSON.stringify({ sub: 'abcdef' + keyIdx, iat, exp }), 'utf-8').toString(
          'base64url'
        ),
      ]

      switch (alg) {
        case 'EdDSA': {
          // Ed25519 signs the raw message directly
          const message = Buffer.from(parts.join('.'))
          parts.push(crypto.sign(null, message, privateKey).toString('base64url'))
          break
        }
        case 'ES256': {
          const sign = crypto.createSign('SHA256')
          sign.write(parts.join('.'))
          sign.end()
          parts.push(
            sign.sign(Object.assign(privateKey, { dsaEncoding: 'ieee-p1363' }), 'base64url')
          )
          break
        }
        case 'RS256': {
          const sign = crypto.createSign('SHA256')
          sign.write(parts.join('.'))
          sign.end()
          parts.push(sign.sign(privateKey, 'base64url'))
          break
        }
        case 'HS256': {
          const hmac = crypto.createHmac('SHA256', privateKey)
          hmac.update(parts.join('.'))
          parts.push(hmac.digest('base64url'))
          break
        }
      }

      const jwtStr = parts.join('.')

      test(`it should verify a JWT with alg=${alg}`, async () => {
        const result = await verifyJWT(jwtStr, hmacPrivateKeyWithoutKid, jwks)
        expect(result.sub).toEqual('abcdef' + keyIdx)
      })
    })

    test('it should try secret if no matching jwk kty/alg found in jwks', async () => {
      const jwk = await generateHS512JWK()
      jwk.kid = 'abc123'
      const sub = 'weird-case-secret'
      const secret = crypto.randomBytes(32).toString('base64url')

      const jwtStr = await new SignJWT({ sub })
        .setIssuedAt()
        .setProtectedHeader({ alg: 'HS256', kid: 'def456' })
        .sign(new TextEncoder().encode(secret))

      const result = await verifyJWT(jwtStr, secret, { keys: [jwk] })
      expect(result.sub).toEqual(sub)
    })

    test('it should use jwt secret if jwks are missing', async () => {
      const jwt = await signJWT({ sub: 'things' }, hmacPrivateKeyWithoutKid, 100)
      const result = await verifyJWT(jwt, hmacPrivateKeyWithoutKid)
      expect(result.sub).toEqual('things')
    })

    test('it should sign and verify using our HS256 generation', async () => {
      const token = await generateHS512JWK()
      token.kid = 'this-is-my-kid'
      const jwt = await signJWT({ sub: 'stuff' }, token, 100)
      const result = await verifyJWT(jwt, 'totally-invalid-secret-not-used', { keys: [token] })
      expect(result.sub).toEqual('stuff')
    })

    test('it should reject if secret is invalid when signing', async () => {
      await expect(signJWT({ sub: 'things' }, '', 100)).rejects.toThrow(
        'Zero-length key is not supported'
      )
    })

    test('it should allow the current maximum numeric expiration and keep exp millisecond-safe', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))

      const maxNumericExpiration = getMaxNumericJWTExpiration()
      const jwt = await signJWT({ sub: 'things' }, hmacPrivateKeyWithoutKid, maxNumericExpiration)
      const result = await verifyJWT(jwt, hmacPrivateKeyWithoutKid)

      expect(maxNumericExpiration).toBeGreaterThan(0)
      expect(Number.isSafeInteger(result.exp)).toBe(true)
      expect(Number.isSafeInteger(result.exp! * 1000)).toBe(true)
    })

    test('it should reject numeric expirations above the current maximum', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))

      const maxNumericExpiration = getMaxNumericJWTExpiration()
      await expect(
        signJWT({ sub: 'things' }, hmacPrivateKeyWithoutKid, maxNumericExpiration + 1)
      ).rejects.toMatchObject({
        code: ErrorCode.InvalidParameter,
        httpStatusCode: 400,
        message: 'Invalid Parameter expiresIn',
      })
    })

    test('it should reject numeric expirations above the current maximum in the shared validator', () => {
      vi.useFakeTimers()
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))

      expect(() => assertValidNumericJWTExpiration(getMaxNumericJWTExpiration() + 1)).toThrow(
        'Invalid Parameter expiresIn'
      )
    })

    test('it should reject numeric expirations below one second', async () => {
      await expect(signJWT({ sub: 'things' }, hmacPrivateKeyWithoutKid, 0)).rejects.toMatchObject({
        code: ErrorCode.InvalidParameter,
        httpStatusCode: 400,
        message: 'Invalid Parameter expiresIn',
      })

      await expect(signJWT({ sub: 'things' }, hmacPrivateKeyWithoutKid, -1)).rejects.toMatchObject({
        code: ErrorCode.InvalidParameter,
        httpStatusCode: 400,
        message: 'Invalid Parameter expiresIn',
      })
    })

    test('it should reject numeric expirations below one second in the shared validator', () => {
      expect(() => assertValidNumericJWTExpiration(0)).toThrow('Invalid Parameter expiresIn')
      expect(() => assertValidNumericJWTExpiration(-1)).toThrow('Invalid Parameter expiresIn')
    })

    test('it should reject if jwt is malformed', async () => {
      await expect(verifyJWT('this is not a jwt', 'and this is not a secret')).rejects.toThrow(
        'Invalid Compact JWS'
      )
    })

    test('it should reuse cached JWT verifications for the same inputs until the token expires', async () => {
      vi.useFakeTimers()
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))

      const recordSpy = vi.spyOn(metrics, 'recordCacheRequest')
      const secret = crypto.randomBytes(32).toString('base64url')
      const token = await signJWT({ sub: 'cached-user' }, secret, 2)

      recordSpy.mockClear()

      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })
      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })

      expect(recordSpy.mock.calls).toEqual([
        [JWT_CACHE_NAME, 'miss'],
        [JWT_CACHE_NAME, 'hit'],
      ])

      vi.advanceTimersByTime(2200)

      await expect(verifyJWTWithCache(token, secret)).rejects.toThrow()
    })

    test('it should not reuse cached JWT verifications when the secret changes', async () => {
      const recordSpy = vi.spyOn(metrics, 'recordCacheRequest')
      const secret = crypto.randomBytes(32).toString('base64url')
      const token = await signJWT({ sub: 'cached-user' }, secret, 2)

      recordSpy.mockClear()

      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })
      await expect(verifyJWTWithCache(token, 'definitely-the-wrong-secret')).rejects.toThrow()

      expect(recordSpy.mock.calls).toEqual([
        [JWT_CACHE_NAME, 'miss'],
        [JWT_CACHE_NAME, 'miss'],
      ])
    })

    test('it should not reuse cached JWT verifications when the JWKS changes', async () => {
      const signingKey = await generateHS512JWK()
      signingKey.kid = 'cache-signing-key'
      const wrongKey = await generateHS512JWK()
      wrongKey.kid = 'wrong-cache-signing-key'
      const token = await signJWT({ sub: 'cached-user' }, signingKey, 2)

      await expect(
        verifyJWTWithCache(token, 'invalid-secret', { keys: [signingKey] })
      ).resolves.toMatchObject({
        sub: 'cached-user',
      })
      await expect(
        verifyJWTWithCache(token, 'invalid-secret', { keys: [wrongKey] })
      ).rejects.toThrow()
    })

    test('it should skip caching when the token expires before the cache ttl is computed', async () => {
      vi.useFakeTimers()

      const issuedAt = new Date('2026-01-01T00:00:00.000Z')
      const issuedAtMs = issuedAt.getTime()
      const tokenExp = issuedAtMs / 1000 + 2
      const secret = 'ttl-edge-secret'
      const token = 'header.payload.signature'

      vi.setSystemTime(issuedAt)
      vi.resetModules()

      const actualJose = await vi.importActual<typeof import('jose')>('jose')
      const jwtVerifyMock = vi
        .fn()
        .mockImplementationOnce(async () => {
          vi.setSystemTime(issuedAtMs + 2000)
          return {
            payload: {
              sub: 'cached-user',
              exp: tokenExp,
            },
          }
        })
        .mockResolvedValue({
          payload: {
            sub: 'cached-user',
            exp: tokenExp,
          },
        })

      vi.doMock('jose', () => ({
        ...actualJose,
        jwtVerify: jwtVerifyMock,
      }))

      try {
        const isolatedMetrics = await import('@internal/monitoring/metrics')
        const { verifyJWTWithCache: isolatedVerifyJWTWithCache } = await import('./jwt')
        const recordSpy = vi.spyOn(isolatedMetrics, 'recordCacheRequest')

        recordSpy.mockClear()

        await expect(isolatedVerifyJWTWithCache(token, secret)).resolves.toMatchObject({
          sub: 'cached-user',
        })

        vi.setSystemTime(issuedAtMs + 1000)

        await expect(isolatedVerifyJWTWithCache(token, secret)).resolves.toMatchObject({
          sub: 'cached-user',
        })
        await expect(isolatedVerifyJWTWithCache(token, secret)).resolves.toMatchObject({
          sub: 'cached-user',
        })

        expect(jwtVerifyMock).toHaveBeenCalledTimes(2)
        expect(recordSpy.mock.calls).toEqual([
          [JWT_CACHE_NAME, 'miss'],
          [JWT_CACHE_NAME, 'miss'],
          [JWT_CACHE_NAME, 'hit'],
        ])
      } finally {
        vi.doUnmock('jose')
        vi.resetModules()
      }
    })
  })

  describe('JWK generation', () => {
    test('generateHS512JWK returns a jwk with the HS512 alg set', async () => {
      const jwk = await generateHS512JWK()

      expect(jwk).toMatchObject({ kty: 'oct', alg: 'HS512' })
      expect(jwk.k).toBeTruthy()
    })

    test('generateECDSA256JWK returns a jwk with the ES256 alg set', async () => {
      const jwk = await generateES256JWK()

      expect(jwk).toMatchObject({ kty: 'EC', crv: 'P-256', alg: 'ES256' })
      expect(jwk.x).toBeTruthy()
      expect(jwk.y).toBeTruthy()
    })

    test('generateHS512JWK produces a jwk that signJWT/verifyJWT can use without an externally supplied alg', async () => {
      const jwk = { ...(await generateHS512JWK()), kid: 'jwk-gen-hs512-kid' }

      const token = await signJWT({ sub: 'jwk-gen-hs512' }, jwk, 100)

      await expect(
        verifyJWT(token, 'unused-fallback-secret', { keys: [jwk] })
      ).resolves.toMatchObject({ sub: 'jwk-gen-hs512' })
    })

    test('generateECDSA256JWK produces a jwk that signJWT/verifyJWT can use without an externally supplied alg', async () => {
      const jwk = { ...(await generateES256JWK()), kid: 'jwk-gen-es256-kid' }

      const token = await signJWT({ sub: 'jwk-gen-es256' }, jwk, 100)

      await expect(
        verifyJWT(token, 'unused-fallback-secret', { keys: [jwk] })
      ).resolves.toMatchObject({ sub: 'jwk-gen-es256' })
    })
  })

  describe('kid stability across a kind swap', () => {
    test('ES256: a jwt signed under the pre-swap kid still verifies once the kind prefix changes', async () => {
      const jwk = await generateES256JWK()
      const id = 'shared-ec-id'
      const signedJwk = { ...jwk, kid: `storage-url-signing-key_${id}` }
      const token = await signJWT({ sub: 'pre-swap' }, signedJwk, 100)

      const postSwapJwk = { ...jwk, kid: `storage-url-standby-key_${id}` }

      await expect(
        verifyJWT(token, 'unused-fallback-secret', { keys: [postSwapJwk] })
      ).resolves.toMatchObject({ sub: 'pre-swap' })
    })

    test('HS512: a jwt signed under the pre-swap kid still verifies once the kind prefix changes', async () => {
      const jwk = await generateHS512JWK()
      const id = 'shared-hs-id'
      const signedJwk = { ...jwk, kid: `storage-url-signing-key_${id}` }
      const token = await signJWT({ sub: 'pre-swap-hs' }, signedJwk, 100)

      const postSwapJwk = { ...jwk, kid: `storage-url-standby-key_${id}` }

      await expect(
        verifyJWT(token, 'totally-invalid-secret-not-used', { keys: [postSwapJwk] })
      ).resolves.toMatchObject({ sub: 'pre-swap-hs' })
    })

    test('does not cross-match two genuinely different keys that happen to share a kind prefix', async () => {
      const jwk = await generateES256JWK()
      const signedJwk = { ...jwk, kid: 'storage-url-signing-key_id-one' }
      const token = await signJWT({ sub: 'should-not-verify' }, signedJwk, 100)

      const unrelatedJwk = {
        ...(await generateES256JWK()),
        kid: 'storage-url-signing-key_id-two',
      }

      await expect(
        verifyJWT(token, 'unused-fallback-secret', { keys: [unrelatedJwk] })
      ).rejects.toThrow()
    })
  })

  describe('toPublicJwk', () => {
    test('EC: strips the private "d" component and keeps only public fields', () => {
      const publicJwk = toPublicJwk({
        kty: 'EC',
        crv: 'P-256',
        x: 'x-value',
        y: 'y-value',
        d: 'private-value',
        kid: 'ec-kid',
        alg: 'ES256',
      } as unknown as JwksConfigKey)

      expect(publicJwk).toEqual({
        kty: 'EC',
        crv: 'P-256',
        x: 'x-value',
        y: 'y-value',
        kid: 'ec-kid',
        alg: 'ES256',
      })
    })

    test('RSA: strips all private components and keeps only public fields', () => {
      const publicJwk = toPublicJwk({
        kty: 'RSA',
        n: 'n-value',
        e: 'e-value',
        d: 'private-d',
        p: 'private-p',
        q: 'private-q',
        dp: 'private-dp',
        dq: 'private-dq',
        qi: 'private-qi',
        kid: 'rsa-kid',
        alg: 'RS256',
      } as unknown as JwksConfigKey)

      expect(publicJwk).toEqual({
        kty: 'RSA',
        n: 'n-value',
        e: 'e-value',
        kid: 'rsa-kid',
        alg: 'RS256',
      })
    })

    test('OKP: strips the private "d" component and keeps only public fields', () => {
      const publicJwk = toPublicJwk({
        kty: 'OKP',
        crv: 'Ed25519',
        x: 'x-value',
        d: 'private-value',
        kid: 'okp-kid',
        alg: 'EdDSA',
      } as unknown as JwksConfigKey)

      expect(publicJwk).toEqual({
        kty: 'OKP',
        crv: 'Ed25519',
        x: 'x-value',
        kid: 'okp-kid',
        alg: 'EdDSA',
      })
    })

    test('drops any field that is not on the public allowlist', () => {
      const publicJwk = toPublicJwk({
        kty: 'EC',
        crv: 'P-256',
        x: 'x-value',
        y: 'y-value',
        someFuturePrivateField: 'should-never-be-published',
      } as unknown as JwksConfigKey)

      expect(publicJwk).not.toHaveProperty('someFuturePrivateField')
      expect(publicJwk).toEqual({ kty: 'EC', crv: 'P-256', x: 'x-value', y: 'y-value' })
    })

    test('throws for "oct" (symmetric) keys, which have no public form', () => {
      expect(() =>
        toPublicJwk({ kty: 'oct', k: 'secret-value' } as unknown as JwksConfigKey)
      ).toThrow('Cannot derive a public jwk for kty "oct"')
    })
  })
})

describe('signed URL scope predicates', () => {
  describe('isUploadScopedToken', () => {
    it('accepts an explicit upload scope', () => {
      expect(isUploadScopedToken({ scope: SIGNED_URL_SCOPE_UPLOAD })).toBe(true)
    })

    it('accepts a legacy upload token (no scope, with upsert)', () => {
      expect(isUploadScopedToken({ upsert: false } as never)).toBe(true)
      expect(isUploadScopedToken({ upsert: true } as never)).toBe(true)
    })

    it('rejects a legacy download-shaped token (no scope, no upsert)', () => {
      expect(isUploadScopedToken({ url: 'b/o' } as never)).toBe(false)
    })

    it('rejects an explicit download scope', () => {
      expect(isUploadScopedToken({ scope: SIGNED_URL_SCOPE_DOWNLOAD })).toBe(false)
    })

    it('rejects an unknown scope even when upsert is present', () => {
      expect(isUploadScopedToken({ scope: 'something-else', upsert: true } as never)).toBe(false)
    })
  })

  describe('isDownloadScopedToken', () => {
    it('accepts an explicit download scope', () => {
      expect(isDownloadScopedToken({ scope: SIGNED_URL_SCOPE_DOWNLOAD })).toBe(true)
    })

    it('accepts a legacy download token (no scope, no upsert)', () => {
      expect(isDownloadScopedToken({ url: 'b/o' } as never)).toBe(true)
    })

    it('rejects a legacy upload-shaped token (no scope, with upsert)', () => {
      expect(isDownloadScopedToken({ upsert: false } as never)).toBe(false)
    })

    it('rejects an explicit upload scope', () => {
      expect(isDownloadScopedToken({ scope: SIGNED_URL_SCOPE_UPLOAD })).toBe(false)
    })

    it('rejects an unknown scope', () => {
      expect(isDownloadScopedToken({ scope: 'something-else' } as never)).toBe(false)
    })
  })
})
