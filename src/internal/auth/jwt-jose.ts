import * as crypto from 'node:crypto'
import { ERRORS } from '@internal/errors'

import { getConfig, JwksConfig, JwksConfigKey, JwksConfigKeyOCT } from '../../config'
import {
  importJWK,
  JWK,
  JWTHeaderParameters,
  JWTPayload,
  jwtVerify,
  JWTVerifyGetKey,
  KeyObject,
  SignJWT,
} from 'jose'

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
): Promise<Uint8Array | CryptoKey | JWK | KeyObject> {
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
  return (header: JWTHeaderParameters) => {
    return findJWKFromHeader(header, secret, jwks)
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
    const { payload } = await jwtVerify<T>(token, getJWTVerificationKey(secret, jwks || null))
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
  payload: any, // TODO
  secret: string | JwksConfigKeyOCT,
  expiresIn: string | number | undefined
): Promise<string> {
  try {
    const signer = new SignJWT(payload).setIssuedAt()
    if (expiresIn) {
      const expiresInStr =
        typeof expiresIn === 'string' ? expiresIn : Math.floor(expiresIn / 1000) + 's'
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
  } catch (e) {
    const err = e as Error
    throw ERRORS.AccessDenied(err.message, err)
  }
}

/**
 * Generate a new random HS256 JWK that can be used for signing JWTs
 */
export function generateHS256JWK(): JwksConfigKeyOCT {
  // Generate a 64-byte random key, convert the secret key to Base64URL encoding (JWK standard)
  const k = crypto.randomBytes(64).toString('base64url')
  return { kty: 'oct', alg: 'HS256', k }
}
