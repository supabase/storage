import { JWT_CACHE_NAME } from '@internal/cache'
import { cacheRequestsTotal } from '@internal/monitoring/metrics'
import * as crypto from 'crypto'
import { SignJWT } from 'jose'
import { JwksConfigKey } from '../config'
import { generateHS512JWK, signJWT, verifyJWT, verifyJWTWithCache } from '../internal/auth'

type JwtTestPublicKey = {
  export: (options?: { format: 'jwk' }) => crypto.JsonWebKey | Record<string, string>
}

describe('JWT', () => {
  describe('verifyJWT with JWKS', () => {
    afterEach(() => {
      jest.restoreAllMocks()
      jest.useRealTimers()
    })

    const keys: {
      type?: string
      options?: object
      alg: string
      kid?: string
      publicKey: JwtTestPublicKey
      privateKey: Buffer | crypto.KeyObject
    }[] = [
      { type: 'rsa', options: { modulusLength: 2048 }, alg: 'RS256' },
      { type: 'ec', options: { namedCurve: 'P-256' }, alg: 'ES256' },
      { type: 'ed25519', options: {}, alg: 'EdDSA' },
    ].map((desc, i) => ({
      kid: i.toString(),
      ...desc,
      ...crypto.generateKeyPairSync(
        desc.type as 'rsa' & 'ec',
        (desc.options || undefined) as object
      ),
    }))

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
            ...publicKey.export({ format: 'jwk' }),
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

      if (!alg.startsWith('HS')) {
        const sign = crypto.createSign('SHA256')
        sign.write(parts.join('.'))
        sign.end()

        if (alg === 'EdDSA') {
          // Ed25519 signs the raw message directly
          const message = Buffer.from(parts.join('.'))
          parts.push(crypto.sign(null, message, privateKey).toString('base64url'))
        } else if (alg === 'ES256') {
          parts.push(
            sign.sign(Object.assign(privateKey, { dsaEncoding: 'ieee-p1363' }), 'base64url')
          )
        } else {
          parts.push(sign.sign(privateKey, 'base64url'))
        }
      } else {
        const hmacAlgo = alg.replace('HS', 'SHA')
        const hmac = crypto.createHmac(hmacAlgo, privateKey)
        hmac.update(parts.join('.'))
        parts.push(hmac.digest('base64url'))
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

    test('it should reject if jwt is malformed', async () => {
      await expect(verifyJWT('this is not a jwt', 'and this is not a secret')).rejects.toThrow(
        'Invalid Compact JWS'
      )
    })

    test('it should reuse cached JWT verifications for the same inputs until the token expires', async () => {
      jest.useFakeTimers()
      jest.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))

      const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
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

      jest.advanceTimersByTime(2200)

      await expect(verifyJWTWithCache(token, secret)).rejects.toThrow()
    })

    test('it should not reuse cached JWT verifications when the secret changes', async () => {
      const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
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
      jest.useFakeTimers()

      const issuedAt = new Date('2026-01-01T00:00:00.000Z')
      const issuedAtMs = issuedAt.getTime()
      const tokenExp = issuedAtMs / 1000 + 2
      const secret = 'ttl-edge-secret'
      const token = 'header.payload.signature'

      jest.setSystemTime(issuedAt)
      jest.resetModules()

      const actualJose = jest.requireActual('jose') as typeof import('jose')
      const jwtVerifyMock = jest
        .fn()
        .mockImplementationOnce(async () => {
          jest.setSystemTime(issuedAtMs + 2000)
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

      jest.doMock('jose', () => ({
        ...actualJose,
        jwtVerify: jwtVerifyMock,
      }))

      try {
        const { cacheRequestsTotal: isolatedCacheRequestsTotal } = await import(
          '@internal/monitoring/metrics'
        )
        const { verifyJWTWithCache: isolatedVerifyJWTWithCache } = await import(
          '../internal/auth/jwt'
        )
        const addSpy = jest.spyOn(isolatedCacheRequestsTotal, 'add')

        addSpy.mockClear()

        await expect(isolatedVerifyJWTWithCache(token, secret)).resolves.toMatchObject({
          sub: 'cached-user',
        })

        jest.setSystemTime(issuedAtMs + 1000)

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
        jest.dontMock('jose')
        jest.resetModules()
      }
    })
  })
})
