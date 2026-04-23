import { getTenantConfig } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { Agent } from 'undici'
import { getConfig } from '../../../config'
import { BaseEvent } from '../base-event'

const {
  isMultitenant,
  webhookURL,
  webhookApiKey,
  webhookQueuePullInterval,
  webhookMaxConnections,
  webhookQueueMaxFreeSockets,
} = getConfig()
const WEBHOOK_TIMEOUT_MS = 4000
const webhookKeepAliveTimeoutMs = Math.min(
  Math.max(webhookQueueMaxFreeSockets, 1) * 1000,
  WEBHOOK_TIMEOUT_MS
)

interface WebhookEvent {
  event: {
    $version: string
    type: string
    region: string
    payload: object & { reqId?: string; sbReqId?: string; bucketId: string; name: string }
    applyTime: number
  }
  sentAt?: string
  tenant: {
    ref: string
    host: string
  }
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
    return new Error(`timeout of ${WEBHOOK_TIMEOUT_MS}ms exceeded`)
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
      signal: AbortSignal.timeout(WEBHOOK_TIMEOUT_MS),
    }

    const response = await fetch(url, requestInit)

    try {
      await assertOkResponse(response)
    } finally {
      await response.body?.cancel().catch(() => {})
    }
  },
}

export class Webhook extends BaseEvent<WebhookEvent> {
  static queueName = 'webhooks'

  protected static getClient() {
    return client
  }

  static getWorkerOptions(): WorkOptions {
    return {
      pollingIntervalSeconds: webhookQueuePullInterval
        ? webhookQueuePullInterval / 1000
        : undefined,
    }
  }

  static getSendOptions(): SendOptions {
    return {
      expireInSeconds: 30,
    }
  }

  static async shouldSend(payload: WebhookEvent) {
    if (isMultitenant) {
      // Do not send an event if disabled for this specific tenant
      const tenant = await getTenantConfig(payload.tenant.ref)
      const disabledEvents = tenant.disableEvents || []
      if (
        disabledEvents.includes(`Webhook:${payload.event.type}`) ||
        disabledEvents.includes(
          `Webhook:${payload.event.type}:${payload.event.payload.bucketId}/${payload.event.payload.name}`
        )
      ) {
        return false
      }
    }

    return true
  }

  static async handle(job: Job<WebhookEvent>) {
    if (!webhookURL) {
      logger.debug('skipping webhook, no WEBHOOK_URL set')
      return job
    }

    const payload = job.data.event.payload as { bucketId?: string; name?: string }
    const path = `${job.data.tenant.ref}/${payload.bucketId}/${payload.name}`

    logSchema.event(logger, `[Lifecycle]: ${job.data.event.type} ${path}`, {
      jodId: job.id,
      type: 'event',
      event: job.data.event.type,
      payload: JSON.stringify(job.data.event.payload),
      objectPath: path,
      resources: ['/' + path],
      tenantId: job.data.tenant.ref,
      project: job.data.tenant.ref,
      reqId: job.data.event.payload.reqId,
      sbReqId: job.data.event.payload.sbReqId,
    })

    try {
      await this.getClient().post(webhookURL, {
        type: 'Webhook',
        event: job.data.event,
        sentAt: new Date(),
        tenant: job.data.tenant,
      })
    } catch (e) {
      const error = normalizeWebhookError(e)

      logger.error(
        {
          error: error.message,
          jodId: job.id,
          type: 'event',
          event: job.data.event.type,
          payload: JSON.stringify(job.data.event.payload),
          objectPath: path,
          resources: [path],
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.event.payload.reqId,
          sbReqId: job.data.event.payload.sbReqId,
        },
        `[Lifecycle]: ${job.data.event.type} ${path} - FAILED`
      )
      throw new Error(
        `Failed to send webhook for event ${job.data.event.type} to ${webhookURL}: ${error.message}`
      )
    }

    return job
  }
}
