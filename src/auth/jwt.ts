import { getJwtSecret as getJwtSecretForTenant } from '../database/tenant'
import jwt from 'jsonwebtoken'
import { getConfig } from '../config'

const { isMultitenant, jwtSecret, jwtAlgorithm } = getConfig()

interface jwtInterface {
  sub?: string
  role?: string
}

export type SignedToken = {
  url: string
  transformations?: string
  exp: number
}

export type SignedUploadToken = {
  owner: string | undefined
  url: string
  exp: number
}

/**
 * Gets the JWT secret key from the env PGRST_JWT_SECRET when running in single-tenant
 * or querying the multi-tenant database by the given tenantId
 * @param tenantId
 */
export async function getJwtSecret(tenantId: string): Promise<string> {
  let secret = jwtSecret
  if (isMultitenant) {
    secret = await getJwtSecretForTenant(tenantId)
  }
  return secret
}

/**
 * Verifies if a JWT is valid
 * @param token
 * @param secret
 */
export function verifyJWT<T>(token: string, secret: string): Promise<jwt.JwtPayload & T> {
  return new Promise((resolve, reject) => {
    jwt.verify(token, secret, { algorithms: [jwtAlgorithm as jwt.Algorithm] }, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded as jwt.JwtPayload & T)
    })
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
  expiresIn: string | number
): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    jwt.sign(
      payload,
      secret,
      { expiresIn, algorithm: jwtAlgorithm as jwt.Algorithm },
      (err, token) => {
        if (err) return reject(err)
        resolve(token)
      }
    )
  })
}

/**
 * Extract the owner (user) from the provided JWT
 * @param token
 * @param secret
 */
export async function getOwner(token: string, secret: string): Promise<string | undefined> {
  const decodedJWT = await verifyJWT(token, secret)
  return (decodedJWT as jwtInterface)?.sub
}

export async function getRole(token: string, secret: string): Promise<string | undefined> {
  const decodedJWT = await verifyJWT(token, secret)
  return (decodedJWT as jwtInterface)?.role
}
