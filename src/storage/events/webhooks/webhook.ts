import { drainResponseBody } from '@internal/http'
import { type LifecycleEventLog, logger } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { Agent, Dispatcher1Wrapper } from 'undici'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'

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

const agent = new Agent({
  allowH2: false,
  connections: webhookMaxConnections,
  // `undici` cannot cap idle socket count like `agentkeepalive.maxFreeSockets`,
  // so use the old knob to make idle sockets expire sooner when a small free pool is desired.
  keepAliveTimeout: webhookKeepAliveTimeoutMs,
  keepAliveMaxTimeout: webhookKeepAliveTimeoutMs,
  headersTimeout: WEBHOOK_TIMEOUT_MS,
  bodyTimeout: WEBHOOK_TIMEOUT_MS,
})
const dispatcher = new Dispatcher1Wrapper(agent)

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
    const requestInit: RequestInit & { dispatcher: typeof dispatcher } = {
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

export class WebhookHandler extends QueueHandler(Webhook) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    pollIdleIntervalMs: webhookQueuePullInterval || 700,
    consumeTimeout: 10_000,
  }

  protected getClient() {
    return client
  }

  async handle(ctx: StorageJob<WebhookEvent>): Promise<void> {
    const { id, data } = ctx.message

    const payload = data.event.payload as { bucketId?: string; name?: string }
    const path = `${data.tenant.ref}/${payload.bucketId}/${payload.name}`

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
      const cause = error?.cause ? ` (${String(error.cause)})` : ''

      logger.error(
        {
          error: error.message + cause,
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

/** What every lifecycle notification carries: the object it concerns, the version it acted on
 * and the net bytes it added to (or removed from) storage. */
export type LifecyclePayload = BasePayload & {
  bucketId: string
  name: string
  version: string | null
  bytesChanged: number
}

/** One lifecycle event to notify about: its type, payload and when it took effect. */
export interface LifecycleWebhook {
  readonly eventType: string
  readonly version: string
  readonly payload: LifecyclePayload
  /** The ordering timestamp for the consuming side (the commit time). */
  readonly applyTime: number
}

/**
 * The lifecycle log line is written where the event happens (the producer), not when the
 * webhook is delivered: it must not depend on the queue, the webhook URL or delivery retries.
 */
function logLifecycleEvent({ eventType, payload }: LifecycleWebhook): void {
  try {
    const path = `${payload.tenant.ref}/${payload.bucketId}/${payload.name}`
    const eventLog: LifecycleEventLog = {
      type: 'event',
      event: eventType,
      payload: JSON.stringify(payload),
      objectPath: path,
      objectVersion: payload.version,
      bytesChanged: payload.bytesChanged,
      resources: ['/' + path],
      tenantId: payload.tenant.ref,
      project: payload.tenant.ref,
      reqId: payload.reqId,
      sbReqId: payload.sbReqId,
    }
    logger.info(eventLog, `[Lifecycle]: ${eventType} ${path}`)
  } catch (e) {
    logger.error(
      { error: e, sbReqId: payload.sbReqId, tenantId: payload.tenant.ref },
      `error logging lifecycle event: ${eventType}`
    )
  }
}

const toWebhookEvent = ({ eventType, version, payload, applyTime }: LifecycleWebhook) => ({
  event: { type: eventType, region, $version: version, applyTime, payload },
  tenant: payload.tenant,
})

function logSendFailure(
  error: unknown,
  { eventType, version, payload, applyTime }: LifecycleWebhook
) {
  logger.error(
    {
      error,
      sbReqId: payload.sbReqId,
      event: {
        type: eventType,
        $version: version,
        applyTime,
        payload: JSON.stringify(payload),
      },
      tenant: payload.tenant,
    },
    `error sending webhook: ${eventType}`
  )
}

/**
 * Wrap any lifecycle payload into the webhooks queue (v1 `BaseEvent.sendWebhook`): errors
 * are logged, never thrown — the business flow is not blocked by webhook delivery.
 * `applyTime` is the ordering timestamp for the consuming side (the commit time).
 */
export async function sendWebhook(
  eventType: string,
  version: string,
  payload: LifecyclePayload,
  applyTime: number
): Promise<void> {
  const webhook: LifecycleWebhook = { eventType, version, payload, applyTime }
  logLifecycleEvent(webhook)
  try {
    await Webhook.send(toWebhookEvent(webhook))
  } catch (e) {
    logSendFailure(e, webhook)
  }
}

/**
 * {@link sendWebhook} for many events at once, as a bulk delete produces: one send to the
 * queue in place of one per object. Resolves once the queue has taken all of them. Like the
 * single form it logs each event where it happens and never throws.
 */
export async function sendWebhookBatch(webhooks: readonly LifecycleWebhook[]): Promise<void> {
  if (webhooks.length === 0) return
  for (const webhook of webhooks) logLifecycleEvent(webhook)
  try {
    await Webhook.batchSend(webhooks.map(toWebhookEvent))
  } catch (e) {
    // The queue reports one error for the send as a whole.
    for (const webhook of webhooks) logSendFailure(e, webhook)
  }
}
