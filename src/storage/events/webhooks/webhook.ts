import { drainResponseBody } from '@internal/http'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { Agent } from 'undici'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { getStorageQueue } from '../queue'
import { defaultRetry, TOPICS } from '../topics'

const {
  region,
  webhookURL,
  webhookApiKey,
  webhookQueuePullInterval,
  webhookMaxConnections,
  webhookQueueMaxFreeSockets,
  pgQueueConcurrentTasksPerQueue,
} = getConfig()

const WEBHOOK_TIMEOUT_MS = 4000
// headersTimeout/bodyTimeout only start counting after connect finishes, but the AbortSignal
// below starts at fetch time and covers connect too — so it needs its own budget on top of
// WEBHOOK_TIMEOUT_MS, or it always wins the race and the dispatcher timeouts can never fire.
const WEBHOOK_CONNECT_TIMEOUT_MS = 2000
const WEBHOOK_TOTAL_TIMEOUT_MS = WEBHOOK_TIMEOUT_MS + WEBHOOK_CONNECT_TIMEOUT_MS
const webhookKeepAliveTimeoutMs = Math.max(webhookQueueMaxFreeSockets, 1) * 1000

export interface WebhookEvent extends BasePayload {
  event: {
    $version: string
    type: string
    region: string
    payload: object & { reqId?: string; sbReqId?: string; bucketId: string; name: string }
    applyTime: number
  }
  sentAt?: string
}

interface WebhookRequest {
  type: 'Webhook'
  event: WebhookEvent['event']
  sentAt: Date
  tenant: WebhookEvent['tenant']
}

interface WebhookClient {
  post(url: string, payload: WebhookRequest): Promise<void>
}

const dispatcher = new Agent({
  connections: webhookMaxConnections,
  // `undici` cannot cap idle socket count like `agentkeepalive.maxFreeSockets`,
  // so use the old knob to make idle sockets expire sooner when a small free pool is desired.
  keepAliveTimeout: webhookKeepAliveTimeoutMs,
  keepAliveMaxTimeout: webhookKeepAliveTimeoutMs,
  headersTimeout: WEBHOOK_TIMEOUT_MS,
  bodyTimeout: WEBHOOK_TIMEOUT_MS,
})

const defaultHeaders = new Headers({
  'content-type': 'application/json',
  ...(webhookApiKey ? { authorization: `Bearer ${webhookApiKey}` } : {}),
})

async function assertOkResponse(response: Response) {
  if (response.ok) {
    return
  }

  throw new Error(`Request failed with status code ${response.status}`)
}

function normalizeWebhookError(error: unknown) {
  if (error instanceof DOMException && error.name === 'TimeoutError') {
    return new Error(`timeout of ${WEBHOOK_TOTAL_TIMEOUT_MS}ms exceeded`)
  }

  if (error instanceof Error) {
    return error
  }

  return new Error(String(error))
}

const client: WebhookClient = {
  async post(url, payload) {
    const requestInit: RequestInit & { dispatcher: Agent } = {
      method: 'POST',
      body: JSON.stringify(payload),
      headers: defaultHeaders,
      dispatcher,
      signal: AbortSignal.timeout(WEBHOOK_TOTAL_TIMEOUT_MS),
    }

    const response = await fetch(url, requestInit)

    try {
      await assertOkResponse(response)
    } finally {
      await drainResponseBody(response)
    }
  },
}

export class Webhook extends storageEvent<WebhookEvent>({
  type: 'Webhook',
  // v1 `Webhook.shouldSend`: gated on the WRAPPED event's type, optionally object-scoped —
  // never on the bare "Webhook" name.
  disableKeys: (data) => [
    `Webhook:${data.event.type}`,
    `Webhook:${data.event.type}:${data.event.payload.bucketId}/${data.event.payload.name}`,
  ],
}) {}

export class WebhookHandler extends TopicHandler(Webhook) {
  override readonly options: SubscribeOptions = {
    prefetch: Math.floor(pgQueueConcurrentTasksPerQueue * 1.2),
    parallelism: pgQueueConcurrentTasksPerQueue,
    pollIdleIntervalMs: webhookQueuePullInterval || 700,
    retry: defaultRetry(TOPICS.webhooks),
  }

  protected getClient() {
    return client
  }

  async handle(ctx: JobContext<WirePayload<WebhookEvent>>): Promise<void> {
    const { id, data } = ctx.message

    const payload = data.event.payload as { bucketId?: string; name?: string }
    const path = `${data.tenant.ref}/${payload.bucketId}/${payload.name}`

    logSchema.event(logger, `[Lifecycle]: ${data.event.type} ${path}`, {
      jobId: id,
      type: 'event',
      event: data.event.type,
      payload: JSON.stringify(data.event.payload),
      objectPath: path,
      resources: ['/' + path],
      tenantId: data.tenant.ref,
      project: data.tenant.ref,
      reqId: data.event.payload.reqId,
      sbReqId: data.event.payload.sbReqId,
    })

    if (!webhookURL) {
      logger.debug('skipping webhook, no WEBHOOK_URL set')
      return
    }

    try {
      await this.getClient().post(webhookURL, {
        type: 'Webhook',
        event: data.event,
        sentAt: new Date(),
        tenant: data.tenant,
      })
    } catch (e) {
      const error = normalizeWebhookError(e)

      logger.error(
        {
          error: error.message,
          jobId: id,
          type: 'event',
          event: data.event.type,
          payload: JSON.stringify(data.event.payload),
          objectPath: path,
          resources: [path],
          tenantId: data.tenant.ref,
          project: data.tenant.ref,
          reqId: data.event.payload.reqId,
          sbReqId: data.event.payload.sbReqId,
        },
        `[Lifecycle]: ${data.event.type} ${path} - FAILED`
      )
      throw new Error(
        `Failed to send webhook for event ${data.event.type} to ${webhookURL}: ${error.message}`
      )
    }
  }
}

/**
 * Wrap any lifecycle payload into the webhooks topic (v1 `BaseEvent.sendWebhook`): errors
 * are logged, never thrown — the business flow is not blocked by webhook delivery.
 */
export async function sendWebhook(
  eventType: string,
  version: string,
  payload: BasePayload & { bucketId: string; name: string }
): Promise<void> {
  try {
    await getStorageQueue().produce(
      new Webhook({
        event: {
          type: eventType,
          region,
          $version: version,
          applyTime: Date.now(),
          payload,
        },
        tenant: payload.tenant,
      })
    )
  } catch (e) {
    logger.error(
      {
        error: e,
        sbReqId: payload.sbReqId,
        event: {
          type: eventType,
          $version: version,
          applyTime: Date.now(),
          payload: JSON.stringify(payload),
        },
        tenant: payload.tenant,
      },
      `error sending webhook: ${eventType}`
    )
  }
}
