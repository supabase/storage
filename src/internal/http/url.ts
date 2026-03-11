function toWellFormedString(value: string): string {
  const maybeToWellFormed = (value as unknown as { toWellFormed?: () => string }).toWellFormed
  if (typeof maybeToWellFormed === 'function') {
    return maybeToWellFormed.call(value)
  }

  let normalized = ''
  for (let i = 0; i < value.length; i++) {
    const currentCodeUnit = value.charCodeAt(i)

    if (currentCodeUnit >= 0xd800 && currentCodeUnit <= 0xdbff) {
      const nextCodeUnit = value.charCodeAt(i + 1)
      if (i + 1 < value.length && nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff) {
        normalized += value[i] + value[i + 1]
        i += 1
      } else {
        normalized += '\uFFFD'
      }
      continue
    }

    if (currentCodeUnit >= 0xdc00 && currentCodeUnit <= 0xdfff) {
      normalized += '\uFFFD'
      continue
    }

    normalized += value[i]
  }

  return normalized
}

export function safeEncodeURIComponent(value: string): string {
  try {
    return encodeURIComponent(value)
  } catch {
    return encodeURIComponent(toWellFormedString(value))
  }
}

export function encodePathPreservingSeparators(path: string): string {
  return path
    .split('/')
    .map((pathToken) => safeEncodeURIComponent(pathToken))
    .join('/')
}

export function encodeBucketAndObjectPath(bucket: string, key: string): string {
  return `${safeEncodeURIComponent(bucket)}/${encodePathPreservingSeparators(key)}`
}

function stripQueryString(rawUrl: string): string {
  const queryIdx = rawUrl.indexOf('?')
  return queryIdx === -1 ? rawUrl : rawUrl.slice(0, queryIdx)
}

export function doesSignedTokenMatchRequestPath(
  rawUrl: string | undefined,
  routePrefix: string,
  signedObjectPath: string
): boolean {
  if (!rawUrl) {
    return false
  }

  const pathname = stripQueryString(rawUrl)
  const expectedPath = `${routePrefix}/${encodePathPreservingSeparators(signedObjectPath)}`
  return pathname === expectedPath
}
