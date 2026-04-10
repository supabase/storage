import { signJWT } from '@internal/auth'
import { getConfig } from '../../../config'

export interface JwtClaims {
  sub?: string
  role?: 'authenticated' | 'anon' | 'service_role' | string
  aud?: string
  exp?: number
  [key: string]: unknown
}

/**
 * Mint a JWT signed with the same secret the storage-api verifies against.
 *
 * Tests that need per-user JWTs (e.g. to exercise RLS) should pass the `sub`
 * they created via `factories.user.create()`. The helper defaults to
 * `role: 'authenticated'` and a 1-hour expiry so tests don't have to spell
 * them out.
 */
export async function mintJWT(claims: JwtClaims = {}): Promise<string> {
  const { jwtSecret } = getConfig()
  const { role = 'authenticated', aud = 'authenticated', ...rest } = claims
  return signJWT({ role, aud, ...rest }, jwtSecret, '1h')
}

export function anonKey(): string {
  const key = process.env.ANON_KEY
  if (!key) throw new Error('ANON_KEY env var is missing — check .env.test')
  return key
}

export async function serviceKey(): Promise<string> {
  const { serviceKeyAsync } = getConfig()
  return serviceKeyAsync
}
