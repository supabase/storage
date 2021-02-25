import { PostgrestClient } from '@supabase/postgrest-js'
import jwt from 'jsonwebtoken'

// @todo define as an interface expecting sub instead
type jwtType =
  | {
      aud: string
      exp: number
      sub: string
      email: string
      app_metadata: object
      user_metadata: object
      role: string
    }
  | undefined

export function getPostgrestClient(jwt: string) {
  const {
    PROJECT_REF: projectRef,
    SUPABASE_DOMAIN: supabaseDomain,
    ANON_KEY: anonKey,
  } = process.env
  if (!anonKey) {
    throw new Error('anonKey not found')
  }
  // @todo in kps, can we just ping localhost?
  const url = `https://${projectRef}.${supabaseDomain}/rest/v1`
  const postgrest = new PostgrestClient(url, {
    headers: {
      apiKey: anonKey,
      Authorization: `Bearer ${jwt}`,
    },
  })
  return postgrest
}

export function verifyJWT(token: string): Promise<object | undefined> {
  const { ANON_KEY: anonKey, JWT_SECRET: jwtSecret } = process.env
  if (!jwtSecret) {
    throw new Error('no jwtsecret')
  }
  return new Promise((resolve, reject) => {
    jwt.verify(token, jwtSecret, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded)
    })
  })
}

export async function getOwner(token: string) {
  const decodedJWT = await verifyJWT(token)
  return (decodedJWT as jwtType)?.sub
}
