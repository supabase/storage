import { createHash } from 'node:crypto'
import {
  createLruCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  JWT_CACHE_NAME,
} from '@internal/cache'
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
import { getConfig, JwksConfig, JwksConfigKey, JwksConfigKeyOCT } from '../../config'

const { jwtAlgorithm } = getConfig()

const JWT_HMAC_ALGOS = ['HS256', 'HS384', 'HS512']
const JWT_RSA_ALGOS = ['RS256', 'RS384', 'RS512']
const JWT_ECC_ALGOS = ['ES256', 'ES384', 'ES512']
const JWT_ED_ALGOS = ['EdDSA']
const MAX_ABSOLUTE_JWT_EXPIRATION_SECONDS = Math.floor(Number.MAX_SAFE_INTEGER / 1000)

/**
 * Scope of a signed URL token. Tokens are bound to a single action so a
 * download token can never be replayed against the upload endpoint (and
 * vice-versa). Legacy tokens issued before this field existed have no scope.
 */
export const SIGNED_URL_SCOPE_DOWNLOAD = 'download'
export const SIGNED_URL_SCOPE_UPLOAD = 'upload'

export type SignedUrlScope = typeof SIGNED_URL_SCOPE_DOWNLOAD | typeof SIGNED_URL_SCOPE_UPLOAD

export type SignedToken = {
  scope?: SignedUrlScope
  url: string
  transformations?: string
  exp: number
}

export type SignedUploadToken = {
  scope?: SignedUrlScope
  owner: string | undefined
  upsert: boolean
  url: string
  exp: number
}

/**
 * Whether a verified signed-URL payload is authorized to **upload**.
 *
 * Accepts tokens explicitly scoped for upload, plus — for backward
 * compatibility — legacy upload tokens issued before scoping existed. Those are
 * identified by the presence of an `upsert` claim, which only the upload-signing
 * flow ever emits. Download-shaped tokens (no `upsert`) and any other scope are
 * rejected, which is what closes the read-token → write-replay hole.
 *
 * Keep this and {@link isDownloadScopedToken} as the single source of truth for
 * signed-URL scope checks — they are duplicated security logic otherwise.
 */
export function isUploadScopedToken(payload: { scope?: SignedUrlScope }): boolean {
  return (
    payload.scope === SIGNED_URL_SCOPE_UPLOAD ||
    (payload.scope === undefined && 'upsert' in payload)
  )
}

/**
 * Whether a verified signed-URL payload is authorized to **download** (read).
 *
 * Accepts tokens explicitly scoped for download, plus legacy download tokens
 * (no scope and no `upsert` claim). Upload tokens, legacy upload-shaped tokens
 * (carrying `upsert`), and any other scope are rejected.
 */
export function isDownloadScopedToken(payload: { scope?: SignedUrlScope }): boolean {
  return (
    payload.scope === SIGNED_URL_SCOPE_DOWNLOAD ||
    (payload.scope === undefined && !('upsert' in payload))
  )
}

const jwtJwksFingerprintCache = new WeakMap<object, string>()
const encoder = new TextEncoder()

async function findJWKFromHeader(
  header: JWTHeaderParameters,
  secret: string,
  jwks: JwksConfig | null
) {
  if (!jwks || !jwks.keys) {
    return encoder.encode(secret)
  }

  if (JWT_HMAC_ALGOS.indexOf(header.alg) > -1) {
    // JWT is using HS, find the proper key

    if (!header.kid && header.alg === jwtAlgorithm) {
      // jwt is probably signed with the static secret
      return encoder.encode(secret)
    }

    // find the first key without a kid or with the matching kid and the "oct" type
    const jwk = jwks.keys.find(
      (key) => (!key.kid || key.kid === header.kid) && key.kty === 'oct' && key.k
    )

    if (!jwk) {
      // jwt is probably signed with the static secret
      return encoder.encode(secret)
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
    return encoder.encode(secret)
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

function getJWTJwksFingerprint(jwks?: { keys: JwksConfigKey[] } | null): string {
  if (!jwks) {
    return 'null'
  }

  const cachedFingerprint = jwtJwksFingerprintCache.get(jwks)
  if (cachedFingerprint) {
    return cachedFingerprint
  }

  const fingerprint = createHash('sha256')
    .update(JSON.stringify(jwks.keys ?? null))
    .digest('base64url')
  jwtJwksFingerprintCache.set(jwks, fingerprint)
  return fingerprint
}

function getJWTCacheKey(token: string, secret: string, jwks?: { keys: JwksConfigKey[] } | null) {
  const hash = createHash('sha256')
    .update(token)
    .update('\0')
    .update(secret)
    .update('\0')
    .update(getJWTJwksFingerprint(jwks))

  return hash.digest('base64url')
}

// JWT payloads are comparatively small and high-churn, so keep a higher
// cardinality guardrail than the longer-lived config-style caches.
// Max 65,536 items. At ~2KB per JWT, this uses roughly ~130MB of heap memory worst-case.
export const JWT_CACHE_MAX_ITEMS = 65536
export const JWT_CACHE_TTL_RESOLUTION_MS = 5000 // 5 seconds

const jwtCache = createLruCache<string, JWTPayload>(JWT_CACHE_NAME, {
  max: JWT_CACHE_MAX_ITEMS,
  ttlResolution: JWT_CACHE_TTL_RESOLUTION_MS,
  purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
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
  const cacheKey = getJWTCacheKey(token, secret, jwks)
  const cachedPayload = jwtCache.get(cacheKey)
  if (cachedPayload && cachedPayload.exp && cachedPayload.exp * 1000 > Date.now()) {
    return Promise.resolve(cachedPayload)
  }

  const payload = await verifyJWT(token, secret, jwks)
  if (!payload.exp) {
    return payload
  }

  const ttl = payload.exp * 1000 - Date.now()
  if (ttl > 0) {
    jwtCache.set(cacheKey, payload, { ttl })
  }
  return payload
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
  if (expiresIn !== undefined) {
    const expiresInStr = getJWTExpirationTime(expiresIn)
    try {
      signer.setExpirationTime(expiresInStr)
    } catch (e) {
      throw ERRORS.InvalidParameter('expiresIn', { error: e as Error })
    }
  }

  if (typeof secret === 'string') {
    const signingSecret = encoder.encode(secret)
    return signer.setProtectedHeader({ alg: jwtAlgorithm }).sign(signingSecret)
  } else {
    const signingSecret = await importJWK(secret)
    return signer
      .setProtectedHeader({ kid: secret.kid, alg: secret.alg || jwtAlgorithm })
      .sign(signingSecret)
  }
}

function getJWTExpirationTime(expiresIn: string | number) {
  if (typeof expiresIn === 'string') {
    return expiresIn
  }

  assertValidNumericJWTExpiration(expiresIn)
  return `${Math.floor(expiresIn)}s`
}

export function getMaxNumericJWTExpiration(nowMs = Date.now()) {
  const nowSeconds = Math.floor(nowMs / 1000)
  return Math.max(0, MAX_ABSOLUTE_JWT_EXPIRATION_SECONDS - nowSeconds)
}

export function assertValidNumericJWTExpiration(expiresIn: number, nowMs = Date.now()) {
  if (!Number.isFinite(expiresIn)) {
    throw ERRORS.InvalidParameter('expiresIn')
  }

  const expiresInSeconds = Math.floor(expiresIn)
  const maxRelativeExpirationSeconds = getMaxNumericJWTExpiration(nowMs)

  if (
    !Number.isSafeInteger(expiresInSeconds) ||
    expiresInSeconds < 1 ||
    expiresInSeconds > maxRelativeExpirationSeconds
  ) {
    throw ERRORS.InvalidParameter('expiresIn')
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
