import * as crypto from 'crypto'

import { verifyJWT } from '../internal/auth'

describe('JWT', () => {
  describe('verifyJWT with JWKS', () => {
    const keys: {
      type?: string
      options?: any
      alg: string
      kid?: string
      publicKey: any
      privateKey: any
    }[] = [
      { type: 'rsa', options: { modulusLength: 2048 }, alg: 'RS256' },
      { type: 'ec', options: { namedCurve: 'P-256' }, alg: 'ES256' },
      // jsonwebtoken does not support ed25519 keys yet
      // { type: 'ed25519', options: null, alg: 'EdDSA' },
    ].map((desc, i) => ({
      kid: i.toString(),
      ...desc,
      ...crypto.generateKeyPairSync(desc.type as any, (desc.options || undefined) as any),
    }))

    const hmacPrivateKeyWithoutKid = crypto.randomBytes(256 / 8).toString('hex')

    // without kid, so the value from the secret argument will be taken
    keys.push({
      alg: 'HS256',
      privateKey: Buffer.from(hmacPrivateKeyWithoutKid, 'utf-8'),
      publicKey: {
        export: (options?: any) => ({
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
        ({ publicKey, kid }) =>
          ({
            ...(publicKey as unknown as crypto.KeyObject).export({ format: 'jwk' }),
            kid,
          } as any)
      ),
    }

    keys.forEach(({ privateKey, alg, kid }) => {
      const iat = Math.trunc(Date.now() / 1000)
      const exp = iat + 60

      const parts = [
        Buffer.from(JSON.stringify({ typ: 'JWT', kid, alg }), 'utf-8').toString('base64url'),
        Buffer.from(JSON.stringify({ sub: 'abcdef', iat, exp }), 'utf-8').toString('base64url'),
      ]

      if (alg !== 'HS256') {
        const sign = crypto.createSign('SHA256')
        sign.write(parts.join('.'))
        sign.end()

        if (alg === 'ES256') {
          parts.push(
            sign.sign(Object.assign(privateKey, { dsaEncoding: 'ieee-p1363' }), 'base64url')
          )
        } else {
          parts.push(sign.sign(privateKey, 'base64url'))
        }
      } else {
        const hmac = crypto.createHmac('SHA256', privateKey)
        hmac.update(parts.join('.'))
        parts.push(hmac.digest('base64url'))
      }

      const jwt = parts.join('.')

      test(`it should verify a JWT with alg=${alg}`, async () => {
        const result = await verifyJWT(jwt, hmacPrivateKeyWithoutKid, jwks)
        expect(result.sub).toEqual('abcdef')
      })
    })
  })
})
