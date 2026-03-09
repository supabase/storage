import { encodePathPreservingSeparators } from '../../storage/path-encoding'

function stripQueryString(rawUrl: string): string {
  const queryIdx = rawUrl.indexOf('?')
  return queryIdx === -1 ? rawUrl : rawUrl.slice(0, queryIdx)
}

function encodeObjectPathForURL(objectPath: string): string {
  return encodePathPreservingSeparators(objectPath)
}

export function doesSignedTokenMatchRequestPath(
  rawUrl: string | undefined,
  routePrefix: string,
  signedObjectPath: string
): boolean {
  if (!rawUrl) {
    return false
  }

  // Verify against the raw URL path to avoid implicit dependencies on framework param decoding.
  const pathname = stripQueryString(rawUrl)
  const expectedPath = `${routePrefix}/${encodeObjectPathForURL(signedObjectPath)}`
  return pathname === expectedPath
}
