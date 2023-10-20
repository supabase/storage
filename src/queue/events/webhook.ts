import { BaseEvent } from './base-event'
import { Job, WorkOptions } from 'pg-boss'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger, logSchema } from '../../monitoring'

const {
  webhookURL,
  webhookApiKey,
  webhookQueuePullInterval,
  webhookQueueTeamSize,
  webhookQueueConcurrency,
} = getConfig()

interface WebhookEvent {
  event: {
    $version: string
    type: string
    payload: object & { reqId?: string }
    applyTime: number
  }
  sentAt: string
  tenant: {
    ref: string
    host: string
  }
}

export class Webhook extends BaseEvent<WebhookEvent> {
  static queueName = 'webhooks'

  static getWorkerOptions(): WorkOptions {
    return {
      newJobCheckInterval: webhookQueuePullInterval,
      teamSize: webhookQueueTeamSize,
      teamConcurrency: webhookQueueConcurrency,
    }
  }

  static async handle(job: Job<WebhookEvent>) {
    if (!webhookURL) {
      logger.info('skipping webhook, no WEBHOOK_URL set')
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
      tenantId: job.data.tenant.ref,
      project: job.data.tenant.ref,
      reqId: job.data.event.payload.reqId,
    })

    try {
      await axios.post(
        webhookURL,
        {
          type: 'Webhook',
          event: job.data.event,
          sentAt: new Date(),
          tenant: job.data.tenant,
        },
        {
          headers: {
            ...(webhookApiKey ? { authorization: `Bearer ${webhookApiKey}` } : {}),
          },
        }
      )
    } catch (e) {
      logger.error(
        {
          error: e,
          jodId: job.id,
          type: 'event',
          event: job.data.event.type,
          payload: JSON.stringify(job.data.event.payload),
          objectPath: path,
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
