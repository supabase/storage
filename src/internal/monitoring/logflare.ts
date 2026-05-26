import { defaultPreparePayload } from 'pino-logflare'

type PayloadMeta = Parameters<typeof defaultPreparePayload>[1]

export function onPreparePayload(payload: Record<string, unknown>, meta: PayloadMeta) {
  const item = defaultPreparePayload(payload, meta)
  item.project = payload.project
  item.request_id = payload.sbReqId
  return item
}

export function onError(_payload: Record<string, unknown>, err: Error) {
  console.error(`[Logflare][Error] ${err.message} - ${err.stack}`)
}
