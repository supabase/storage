import type { IncomingHttpHeaders } from 'node:http'

// This transport contract is shared with API Gateway.
// It doesn't affect authentication or authorization.
export const GATEWAY_ENCODING_HEADER = 'sb-encoding-protocol'
export const OPAQUE_ENCODING_PROTOCOL = 'opaque-v1'
export const STORED_CONTENT_ENCODING_HEADER = 'sb-content-encoding'

export function usesOpaqueContentEncoding(headers: IncomingHttpHeaders): boolean {
  return headers[GATEWAY_ENCODING_HEADER] === OPAQUE_ENCODING_PROTOCOL
}

export function withEncodingProtocolVary(vary?: string): string {
  return vary ? `${vary}, ${GATEWAY_ENCODING_HEADER}` : GATEWAY_ENCODING_HEADER
}

export function withOpaqueContentEncoding(
  headers: Record<string, string>,
  enabled = false
): Record<string, string> {
  headers.vary = withEncodingProtocolVary(headers.vary)
  const encoding = headers['content-encoding']
  if (enabled && encoding) {
    headers[STORED_CONTENT_ENCODING_HEADER] = encoding
    delete headers['content-encoding']
  }
  return headers
}
