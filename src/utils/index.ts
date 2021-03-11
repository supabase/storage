import { PostgrestClient } from '@supabase/postgrest-js'
import jwt from 'jsonwebtoken'
import { PostgrestError, StorageError } from '../types/types'
import { getConfig } from '../utils/config'
const { postgrestURL, anonKey, jwtSecret } = getConfig()

// @todo define as an interface expecting sub instead
type jwtType =
  | {
      aud: string
      exp: number
      sub: string
      email: string
      app_metadata: Record<string, unknown>
      user_metadata: Record<string, unknown>
      role: string
    }
  | undefined

export function getPostgrestClient(jwt: string): PostgrestClient {
  // @todo in kps, can we just ping localhost?
  const postgrest = new PostgrestClient(postgrestURL, {
    headers: {
      apiKey: anonKey,
      Authorization: `Bearer ${jwt}`,
    },
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
  return (decodedJWT as jwtType)?.sub
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
  } else {
    console.log(error, responseStatus)
  }
  return {
    statusCode: code,
    error: type,
    message,
  }
}
