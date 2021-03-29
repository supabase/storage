import { PostgrestClient } from '@supabase/postgrest-js'
import jwt from 'jsonwebtoken'
import { PostgrestError, StorageError } from '../types/types'
import { getConfig } from '../utils/config'
const { postgrestURL, anonKey, jwtSecret } = getConfig()

interface jwtInterface {
  sub: string
}

export function getPostgrestClient(jwt: string): PostgrestClient {
  const postgrest = new PostgrestClient(postgrestURL, {
    headers: {
      apiKey: anonKey,
      Authorization: `Bearer ${jwt}`,
    },
    schema: 'storage',
  })
  return postgrest
}

// eslint-disable-next-line @typescript-eslint/ban-types
export function verifyJWT(token: string): Promise<object | undefined> {
  return new Promise((resolve, reject) => {
    jwt.verify(token, jwtSecret, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded)
    })
  })
}

export function signJWT(
  // eslint-disable-next-line @typescript-eslint/ban-types
  payload: string | object | Buffer,
  expiresIn: string | number
): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    jwt.sign(payload, jwtSecret, { expiresIn }, (err, token) => {
      if (err) return reject(err)
      resolve(token)
    })
  })
}

export async function getOwner(token: string): Promise<string | undefined> {
  const decodedJWT = await verifyJWT(token)
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

export function isValidKey(key: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  return /^(\w|\/|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/.test(key)
}
