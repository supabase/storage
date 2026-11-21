import { ERRORS } from '@internal/errors'

const AMZ_HEADER_PREFIX = 'x-amz-'

// Headers exempt from the "must be signed if present" guard:
// x-amz-content-sha256 is checked against the body directly, so it does not need to be signed.
// x-amz-cf-id is unconditionally injected by AWS CloudFront on every request it forwards to a custom origin.
// cache-control can never be signed by standard SigV4 clients (see ALWAYS_UNSIGNABLE_HEADERS in @smithy/signature-v4)
const UNSIGNED_SAFE_HEADER_NAMES = new Set(['x-amz-content-sha256', 'cache-control', 'x-amz-cf-id'])

export function isGuardedAmzHeaderName(name: string) {
  if (UNSIGNED_SAFE_HEADER_NAMES.has(name)) {
    return false
  }
  return name.startsWith(AMZ_HEADER_PREFIX)
}

// x-amz-acl decides whether CreateBucket makes the bucket public.
export function isGuardedCreateBucketHeaderName(name: string) {
  return name === 'x-amz-acl'
}

export function assertGuardedHeadersSigned(
  headers: Record<string, unknown>,
  signedHeaders: Set<string> | undefined,
  isGuarded: (name: string) => boolean
) {
  for (const name of Object.keys(headers)) {
    if (!isGuarded(name)) {
      continue
    }
    if (headers[name] === undefined) {
      continue
    }
    if (!signedHeaders?.has(name)) {
      throw ERRORS.AccessDenied(
        `The "${name}" header must be included in the request's SignedHeaders`
      )
    }
  }
}
