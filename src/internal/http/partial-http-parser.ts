import { getConfig } from '../../config'

const { isMultitenant, requestXForwardedHostRegExp } = getConfig()

const REQUEST_LINE_REGEX = /^([A-Z]+)\s+(\S+)(?:\s+HTTP\/[\d.]+)?$/i
const LINE_SPLIT_REGEX = /\r?\n/
// Validate header name (RFC 7230 token characters)
const HEADER_NAME_REGEX = /^[a-z0-9!#$%&'*+\-.^_`|~]+$/

const MAX_HEADER_LINES = 100

export interface PartialHttpData {
  method: string
  url: string
  headers: Record<string, string>
  tenantId: string
  length: number
}

/**
 * Parses partial HTTP request data from raw buffers.
 * Returns defaults if parsing fails.
 */
export function parsePartialHttp(dataChunks: Buffer[]): PartialHttpData {
  const result: PartialHttpData = {
    method: 'UNKNOWN',
    url: '/',
    headers: {},
    tenantId: isMultitenant ? 'unknown' : 'storage-single-tenant',
    length: 0,
  }

  if (dataChunks.length === 0) {
    return result
  }

  try {
    const partialData = Buffer.concat(dataChunks).toString('utf8')
    const lines = partialData.split(LINE_SPLIT_REGEX)
    result.length = partialData.length

    // Parse request line: "METHOD /path HTTP/version"
    if (lines[0]) {
      const requestLine = lines[0].match(REQUEST_LINE_REGEX)
      if (requestLine) {
        result.method = requestLine[1].toUpperCase()
        result.url = requestLine[2]
      }
    }

    // Parse headers (skip line 0, limit total lines)
    const headerLineLimit = Math.min(lines.length, MAX_HEADER_LINES + 1)
    for (let i = 1; i < headerLineLimit; i++) {
      const line = lines[i]
      if (!line || line.trim() === '') continue

      const colonIndex = line.indexOf(':')
      if (colonIndex > 0) {
        const field = line.substring(0, colonIndex).trim().toLowerCase()
        const value = line.substring(colonIndex + 1).trim()
        if (HEADER_NAME_REGEX.test(field)) {
          result.headers[field] = value
        }
      }
    }

    // Extract tenantId from x-forwarded-host if multitenant
    if (isMultitenant && requestXForwardedHostRegExp && result.headers['x-forwarded-host']) {
      const match = result.headers['x-forwarded-host'].match(requestXForwardedHostRegExp)
      if (match && match[1]) {
        result.tenantId = match[1]
      }
    }
  } catch {
    // Parsing failed - return defaults
    // This catches malformed UTF-8, regex errors, etc.
  }

  return result
}
