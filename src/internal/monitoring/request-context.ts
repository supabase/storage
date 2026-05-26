import type { IncomingHttpHeaders } from 'node:http'

export const SUPABASE_REQUEST_ID_HEADER = 'sb-request-id'

export function getSbReqId(headers: IncomingHttpHeaders): string | undefined {
  const sbReqId = headers[SUPABASE_REQUEST_ID_HEADER]

  return getNonEmptyString(sbReqId)
}

export function getSbReqIdFromPayload(payload: unknown): string | undefined {
  if (!payload || typeof payload !== 'object') {
    return undefined
  }

  const directSbReqId = getNonEmptyString((payload as { sbReqId?: unknown }).sbReqId)

  if (directSbReqId) {
    return directSbReqId
  }

  // Webhook payloads wrap the original event under `event.payload`
  // every other job carries sbReqId at the top level.
  const nestedSbReqId = getNonEmptyString(
    (payload as { event?: { payload?: { sbReqId?: unknown } } }).event?.payload?.sbReqId
  )

  return nestedSbReqId
}

function getNonEmptyString(value: unknown): string | undefined {
  if (Array.isArray(value)) {
    return getNonEmptyString(value[0])
  }

  if (typeof value !== 'string' || value.length === 0) {
    return undefined
  }

  return value
}
