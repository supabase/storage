import { BaseEvent } from './base-event'
import { Job, WorkOptions } from 'pg-boss'
import { HttpsAgent } from 'agentkeepalive'
import HttpAgent from 'agentkeepalive'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger, logSchema } from '@internal/monitoring'
import { getTenantConfig } from '@internal/database'

const {
  isMultitenant,
  webhookURL,
  webhookApiKey,
  webhookQueuePullInterval,
  webhookQueueTeamSize,
  webhookQueueConcurrency,
  webhookMaxConnections,
  webhookQueueMaxFreeSockets,
} = getConfig()

interface WebhookEvent {
  event: {
    $version: string
    type: string
    payload: object & { reqId?: string; bucketId: string; name: string }
    applyTime: number
  }
  sentAt: string
  tenant: {
    ref: string
    host: string
  }
}

const httpAgent = webhookURL?.startsWith('https://')
  ? {
      httpsAgent: new HttpsAgent({
        maxSockets: webhookMaxConnections,
        maxFreeSockets: webhookQueueMaxFreeSockets,
      }),
    }
  : {
      httpAgent: new HttpAgent({
        maxSockets: webhookMaxConnections,
        maxFreeSockets: webhookQueueMaxFreeSockets,
      }),
    }

const client = axios.create({
  ...httpAgent,
  timeout: 4000,
  headers: {
    ...(webhookApiKey ? { authorization: `Bearer ${webhookApiKey}` } : {}),
  },
})

export class Webhook extends BaseEvent<WebhookEvent> {
  static queueName = 'webhooks'

  static getWorkerOptions(): WorkOptions {
    return {
      newJobCheckInterval: webhookQueuePullInterval,
      teamSize: webhookQueueTeamSize,
      teamConcurrency: webhookQueueConcurrency,
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
    })

    try {
      await client.post(webhookURL, {
        type: 'Webhook',
        event: job.data.event,
        sentAt: new Date(),
        tenant: job.data.tenant,
      })
    } catch (e) {
      logger.error(
        {
          error: (e as any)?.message,
          jodId: job.id,
          type: 'event',
          event: job.data.event.type,
          payload: JSON.stringify(job.data.event.payload),
          objectPath: path,
          resources: [path],
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.event.payload.reqId,
        },
        `[Lifecycle]: ${job.data.event.type} ${path} - FAILED`
      )
      throw e
    }

    return job
  }
}
