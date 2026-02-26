import { ERRORS } from '@internal/errors'
import {
  exportJWK,
  generateSecret,
  importJWK,
  JWTHeaderParameters,
  JWTPayload,
  JWTVerifyGetKey,
  jwtVerify,
  SignJWT,
} from 'jose'
import { LRUCache } from 'lru-cache'
import objectSizeOf from 'object-sizeof'
import { getConfig, JwksConfig, JwksConfigKey, JwksConfigKeyOCT } from '../../config'

const { jwtAlgorithm } = getConfig()

const JWT_HMAC_ALGOS = ['HS256', 'HS384', 'HS512']
const JWT_RSA_ALGOS = ['RS256', 'RS384', 'RS512']
const JWT_ECC_ALGOS = ['ES256', 'ES384', 'ES512']
const JWT_ED_ALGOS = ['EdDSA']

export type SignedToken = {
  url: string
  transformations?: string
  exp: number
}

export type SignedUploadToken = {
  owner: string | undefined
  upsert: boolean
  url: string
  exp: number
}

async function findJWKFromHeader(
  header: JWTHeaderParameters,
  secret: string,
  jwks: JwksConfig | null
) {
  if (!jwks || !jwks.keys) {
    return new TextEncoder().encode(secret)
  }

  if (JWT_HMAC_ALGOS.indexOf(header.alg) > -1) {
    // JWT is using HS, find the proper key

    if (!header.kid && header.alg === jwtAlgorithm) {
      // jwt is probably signed with the static secret
      return new TextEncoder().encode(secret)
    }

    // find the first key without a kid or with the matching kid and the "oct" type
    const jwk = jwks.keys.find(
      (key) => (!key.kid || key.kid === header.kid) && key.kty === 'oct' && key.k
    )

    if (!jwk) {
      // jwt is probably signed with the static secret
      return new TextEncoder().encode(secret)
    }

    return Buffer.from(jwk.k, 'base64')
  }

  // jwt is using an asymmetric algorithm
  let kty = 'RSA'

  if (JWT_ECC_ALGOS.indexOf(header.alg) > -1) {
    kty = 'EC'
  } else if (JWT_ED_ALGOS.indexOf(header.alg) > -1) {
    kty = 'OKP'
  }

  // find the first key with a matching kid (or no kid if none is specified in the JWT header) and the correct key type
  const jwk = jwks.keys.find((key) => {
    return ((!key.kid && !header.kid) || key.kid === header.kid) && key.kty === kty
  })

  if (!jwk) {
    // couldn't find a matching JWK, try to use the secret
    return new TextEncoder().encode(secret)
  }
  return await importJWK(jwk)
}

function getJWTVerificationKey(secret: string, jwks: JwksConfig | null): JWTVerifyGetKey {
  return (header: JWTHeaderParameters) => findJWKFromHeader(header, secret, jwks)
}

function getJWTAlgorithms(jwks: JwksConfig | null) {
  let algorithms: string[]

  if (jwks && jwks.keys && jwks.keys.length) {
    const hasRSA = jwks.keys.find((key) => key.kty === 'RSA')
    const hasECC = jwks.keys.find((key) => key.kty === 'EC')
    const hasED = jwks.keys.find(
      (key) => key.kty === 'OKP' && (key.crv === 'Ed25519' || key.crv === 'Ed448')
    )
    const hasHS = jwks.keys.find((key) => key.kty === 'oct' && key.k)

    algorithms = [
      jwtAlgorithm,
      ...(hasRSA ? JWT_RSA_ALGOS : []),
      ...(hasECC ? JWT_ECC_ALGOS : []),
      ...(hasED ? JWT_ED_ALGOS : []),
      ...(hasHS ? JWT_HMAC_ALGOS : []),
    ]
  } else {
    algorithms = [jwtAlgorithm]
  }

  return algorithms
}

const jwtCache = new LRUCache<string, { token: string; payload: JWTPayload }>({
  maxSize: 1024 * 1024 * 50, // 50MB
  sizeCalculation: (value) => {
    return objectSizeOf(value)
  },
  ttlResolution: 5000, // 5 seconds
})

/**
 * Verifies if a JWT is valid and caches the payload
 * for the duration of the token's expiration time
 * @param token
 * @param secret
 * @param jwks
 */
export async function verifyJWTWithCache(
  token: string,
  secret: string,
  jwks?: { keys: JwksConfigKey[] } | null
) {
  const cachedVerification = jwtCache.get(token)
  if (
    cachedVerification &&
    cachedVerification.payload.exp &&
    cachedVerification.payload.exp * 1000 > Date.now()
  ) {
    return Promise.resolve(cachedVerification.payload)
  }

  try {
    const payload = await verifyJWT(token, secret, jwks)
    if (!payload.exp) {
      return payload
    }

    jwtCache.set(
      token,
      { token, payload },
      {
        ttl: payload.exp * 1000 - Date.now(),
      }
    )
    return payload
  } catch (e) {
    throw e
  }
}

/**
 * Verifies if a JWT is valid
 * @param token
 * @param secret
 * @param jwks
 */
export async function verifyJWT<T>(
  token: string,
  secret: string,
  jwks?: { keys: JwksConfigKey[] } | null
): Promise<JWTPayload & T> {
  try {
    const { payload } = await jwtVerify<T>(token, getJWTVerificationKey(secret, jwks || null), {
      algorithms: getJWTAlgorithms(jwks || null),
    })
    return payload
  } catch (e) {
    const err = e as Error
    throw ERRORS.AccessDenied(err.message, err)
  }
}

/**
 * Sign a JWT
 * @param payload
 * @param secret
 * @param expiresIn
 */
export async function signJWT(
  payload: JWTPayload,
  secret: string | JwksConfigKeyOCT,
  expiresIn: string | number | undefined
): Promise<string> {
  const signer = new SignJWT(payload).setIssuedAt()
  if (expiresIn) {
    const expiresInStr = typeof expiresIn === 'string' ? expiresIn : Math.floor(expiresIn) + 's'
    signer.setExpirationTime(expiresInStr)
  }

  if (typeof secret === 'string') {
    const signingSecret = new TextEncoder().encode(secret)
    return signer.setProtectedHeader({ alg: jwtAlgorithm }).sign(signingSecret)
  } else {
    const signingSecret = await importJWK(secret)
    return signer
      .setProtectedHeader({ kid: secret.kid, alg: secret.alg || jwtAlgorithm })
      .sign(signingSecret)
  }
}

/**
 * Generate a new random HS512 JWK that can be used for signing JWTs
 */
export async function generateHS512JWK(): Promise<JwksConfigKeyOCT> {
  const secret = await generateSecret('HS512', { extractable: true })
  return (await exportJWK(secret)) as JwksConfigKeyOCT
}

const JWT_SHAPE =
  /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)?$/

export function isJwtToken(token: string) {
  return token.replace('Bearer ', '').match(JWT_SHAPE)
}
