import * as crypto from 'crypto'
import { generateHS512JWK, signJWT, verifyJWT } from '../internal/auth'
import { JwksConfigKey } from '../config'
import { SignJWT } from 'jose'

describe('JWT', () => {
  describe('verifyJWT with JWKS', () => {
    const keys: {
      type?: string
      options?: object
      alg: string
      kid?: string
      // biome-ignore lint/suspicious/noExplicitAny: Crypto key variants are intentionally broad in this test matrix.
      publicKey: any
      // biome-ignore lint/suspicious/noExplicitAny: Crypto key variants are intentionally broad in this test matrix.
      privateKey: any
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
            ...(publicKey as unknown as crypto.KeyObject).export({ format: 'jwk' }),
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
  })
})
