import * as crypto from 'node:crypto'
import jwt from 'jsonwebtoken'
import { ERRORS } from '@internal/errors'

import { getConfig } from '../../config'

const { jwtAlgorithm } = getConfig()

const JWT_HMAC_ALGOS: jwt.Algorithm[] = ['HS256', 'HS384', 'HS512']
const JWT_RSA_ALGOS: jwt.Algorithm[] = ['RS256', 'RS384', 'RS512']
const JWT_ECC_ALGOS: jwt.Algorithm[] = ['ES256', 'ES384', 'ES512']
const JWT_ED_ALGOS: jwt.Algorithm[] = ['EdDSA'] as unknown as jwt.Algorithm[] // types for EdDSA not yet updated

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

export function findJWKFromHeader(
  header: jwt.JwtHeader,
  secret: string,
  jwks: { keys: { kid?: string; kty: string }[] } | null
) {
  if (!jwks || !jwks.keys) {
    return secret
  }

  if (JWT_HMAC_ALGOS.indexOf(header.alg as jwt.Algorithm) > -1) {
    // JWT is using HS, find the proper key

    if (!header.kid && header.alg === jwtAlgorithm) {
      // jwt is probably signed with the static secret
      return secret
    }

    // find the first key without a kid or with the matching kid and the "oct" type
    const jwk = jwks.keys.find(
      (key) => (!key.kid || key.kid === header.kid) && key.kty === 'oct' && (key as any).k
    )

    if (!jwk) {
      // jwt is probably signed with the static secret
      return secret
    }

    return Buffer.from((jwk as any).k, 'base64')
  }

  // jwt is using an asymmetric algorithm
  let kty = 'RSA'

  if (JWT_ECC_ALGOS.indexOf(header.alg as jwt.Algorithm) > -1) {
    kty = 'EC'
  } else if (JWT_ED_ALGOS.indexOf(header.alg as jwt.Algorithm) > -1) {
    kty = 'OKP'
  }

  // find the first key with a matching kid (or no kid if none is specified in the JWT header) and the correct key type
  const jwk = jwks.keys.find((key) => {
    return ((!key.kid && !header.kid) || key.kid === header.kid) && key.kty === kty
  })

  if (!jwk) {
    // couldn't find a matching JWK, try to use the secret
    return secret
  }

  return crypto.createPublicKey({
    format: 'jwk',
    key: jwk,
  })
}

function getJWTVerificationKey(
  secret: string,
  jwks: { keys: { kid?: string; kty: string }[] } | null
): jwt.GetPublicKeyOrSecret {
  return (header: jwt.JwtHeader, callback: jwt.SigningKeyCallback) => {
    let result: any = null

    try {
      result = findJWKFromHeader(header, secret, jwks)
    } catch (e: any) {
      callback(e)
      return
    }

    callback(null, result)
  }
}

export function getJWTAlgorithms(jwks: { keys: { kid?: string; kty: string }[] } | null) {
  let algorithms: jwt.Algorithm[]

  if (jwks && jwks.keys && jwks.keys.length) {
    const hasRSA = jwks.keys.find((key) => key.kty === 'RSA')
    const hasECC = jwks.keys.find((key) => key.kty === 'EC')
    const hasED = jwks.keys.find(
      (key) => key.kty === 'OKP' && ((key as any).crv === 'Ed25519' || (key as any).crv === 'Ed448')
    )
    const hasHS = jwks.keys.find((key) => key.kty === 'oct' && (key as any).k)

    algorithms = [
      jwtAlgorithm as jwt.Algorithm,
      ...(hasRSA ? JWT_RSA_ALGOS : []),
      ...(hasECC ? JWT_ECC_ALGOS : []),
      ...(hasED ? JWT_ED_ALGOS : []),
      ...(hasHS ? JWT_HMAC_ALGOS : []),
    ]
  } else {
    algorithms = [jwtAlgorithm as jwt.Algorithm]
  }

  return algorithms
}

/**
 * Verifies if a JWT is valid
 * @param token
 * @param secret
 * @param jwks
 */
export function verifyJWT<T>(
  token: string,
  secret: string,
  jwks?: { keys: { kid?: string; kty: string }[] } | null
): Promise<jwt.JwtPayload & T> {
  return new Promise((resolve, reject) => {
    jwt.verify(
      token,
      getJWTVerificationKey(secret, jwks || null),
      { algorithms: getJWTAlgorithms(jwks || null) },
      (err, decoded) => {
        if (err) return reject(ERRORS.AccessDenied(err.message, err))
        resolve(decoded as jwt.JwtPayload & T)
      }
    )
  })
}

/**
 * Sign a JWT
 * @param payload
 * @param secret
 * @param expiresIn
 */
export function signJWT(
  payload: string | object | Buffer,
  secret: string,
  expiresIn: string | number | undefined
): Promise<string> {
  const options: jwt.SignOptions = { algorithm: jwtAlgorithm as jwt.Algorithm }

  if (expiresIn) {
    options.expiresIn = expiresIn
  }

  return new Promise<string>((resolve, reject) => {
    jwt.sign(payload, secret, options, (err, token) => {
      if (err) return reject(err)
      resolve(token as string)
    })
  })
}
