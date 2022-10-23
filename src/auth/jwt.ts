import { getJwtSecret as getJwtSecretForTenant } from '../database/tenant'
import jwt from 'jsonwebtoken'
import { getConfig } from '../config'

const { isMultitenant, jwtSecret } = getConfig()

interface jwtInterface {
  sub: string
}

export type SignedToken = {
  url: string
}

export async function getJwtSecret(tenantId: string): Promise<string> {
  let secret = jwtSecret
  if (isMultitenant) {
    secret = await getJwtSecretForTenant(tenantId)
  }
  return secret
}

// eslint-disable-next-line @typescript-eslint/ban-types
export function verifyJWT(
  token: string,
  secret: string
): Promise<string | jwt.JwtPayload | undefined> {
  return new Promise((resolve, reject) => {
    jwt.verify(token, secret, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded)
    })
  })
}

export function signJWT(
  // eslint-disable-next-line @typescript-eslint/ban-types
  payload: string | object | Buffer,
  secret: string,
  expiresIn: string | number
): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    jwt.sign(payload, secret, { expiresIn }, (err, token) => {
      if (err) return reject(err)
      resolve(token)
    })
  })
}

export async function getOwner(token: string, secret: string): Promise<string | undefined> {
  const decodedJWT = await verifyJWT(token, secret)
  return (decodedJWT as jwtInterface)?.sub
}
