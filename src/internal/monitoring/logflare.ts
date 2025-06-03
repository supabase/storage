import { defaultPreparePayload } from 'pino-logflare'
import { PayloadMeta } from 'pino-logflare/dist/httpStream'

export function onPreparePayload(payload: Record<string, object>, meta: PayloadMeta) {
  const item = defaultPreparePayload(payload, meta)
  item.project = payload.project
  return item
}

export function onError(_payload: Record<string, object>, err: Error) {
  console.error(`[Logflare][Error] ${err.message} - ${err.stack}`)
}
