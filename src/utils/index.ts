import jwt from 'jsonwebtoken'
import { PostgrestError, StorageError } from '../types/types'
import { getConfig } from '../utils/config'
import {
  getFileSizeLimit as getFileSizeLimitForTenant,
  getJwtSecret as getJwtSecretForTenant,
} from './tenant'

const { isMultitenant, jwtSecret } = getConfig()

interface jwtInterface {
  sub: string
}

export async function getFileSizeLimit(tenantId: string): Promise<number> {
  let { fileSizeLimit } = getConfig()
  if (isMultitenant) {
    fileSizeLimit = await getFileSizeLimitForTenant(tenantId)
  }
  return fileSizeLimit
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

export function transformPostgrestError(
  error: PostgrestError,
  responseStatus: number
): StorageError {
  let { message, details: type, code } = error
  if (responseStatus === 406) {
    code = '404'
    message = 'The resource was not found'
    type = 'Not found'
  } else if (responseStatus === 401) {
    code = '401'
    type = 'Invalid JWT'
  }
  return {
    statusCode: code,
    error: type,
    message,
  }
}

export function normalizeContentType(contentType: string | undefined): string | undefined {
  if (contentType?.includes('text/html')) {
    return 'text/plain'
  }
  return contentType
}

export function isValidKey(key: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  return key.length > 0 && /^(\w|\/|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/.test(key)
}
