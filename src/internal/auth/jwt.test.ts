import { JWT_CACHE_NAME } from '@internal/cache'
import { ErrorCode } from '@internal/errors'
import { cacheRequestsTotal } from '@internal/monitoring/metrics'
import * as crypto from 'crypto'
import { SignJWT } from 'jose'
import { vi } from 'vitest'
import { JwksConfigKey } from '../../config'
import {
  assertValidNumericJWTExpiration,
  generateHS512JWK,
  getMaxNumericJWTExpiration,
  signJWT,
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

      const addSpy = vi.spyOn(cacheRequestsTotal, 'add')
      const secret = crypto.randomBytes(32).toString('base64url')
      const token = await signJWT({ sub: 'cached-user' }, secret, 2)

      addSpy.mockClear()

      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })
      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })

      expect(addSpy.mock.calls).toEqual([
        [1, { cache: JWT_CACHE_NAME, outcome: 'miss' }],
        [1, { cache: JWT_CACHE_NAME, outcome: 'hit' }],
      ])

      vi.advanceTimersByTime(2200)

      await expect(verifyJWTWithCache(token, secret)).rejects.toThrow()
    })

    test('it should not reuse cached JWT verifications when the secret changes', async () => {
      const addSpy = vi.spyOn(cacheRequestsTotal, 'add')
      const secret = crypto.randomBytes(32).toString('base64url')
      const token = await signJWT({ sub: 'cached-user' }, secret, 2)

      addSpy.mockClear()

      await expect(verifyJWTWithCache(token, secret)).resolves.toMatchObject({
        sub: 'cached-user',
      })
      await expect(verifyJWTWithCache(token, 'definitely-the-wrong-secret')).rejects.toThrow()

      expect(addSpy.mock.calls).toEqual([
        [1, { cache: JWT_CACHE_NAME, outcome: 'miss' }],
        [1, { cache: JWT_CACHE_NAME, outcome: 'miss' }],
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
        const { cacheRequestsTotal: isolatedCacheRequestsTotal } = await import(
          '@internal/monitoring/metrics'
        )
        const { verifyJWTWithCache: isolatedVerifyJWTWithCache } = await import('./jwt')
        const addSpy = vi.spyOn(isolatedCacheRequestsTotal, 'add')

        addSpy.mockClear()

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
        expect(addSpy.mock.calls).toEqual([
          [1, { cache: JWT_CACHE_NAME, outcome: 'miss' }],
          [1, { cache: JWT_CACHE_NAME, outcome: 'miss' }],
          [1, { cache: JWT_CACHE_NAME, outcome: 'hit' }],
        ])
      } finally {
        vi.doUnmock('jose')
        vi.resetModules()
      }
    })
  })
})
