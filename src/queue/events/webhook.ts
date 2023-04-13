import { BaseEvent } from './base-event'
import { Job, WorkOptions } from 'pg-boss'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger } from '../../monitoring'
import rateLimit from 'axios-rate-limit'

const { webhookURL, webhookApiKey, webhookQueuePullInterval, webhookQueueBatchSize } = getConfig()

const httpClient = rateLimit(axios.create(), { maxRPS: 100 })

interface WebhookEvent {
  event: {
    $version: string
    type: string
    payload: object
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
      batchSize: webhookQueueBatchSize,
    }
  }

  static async handle(job: Job<WebhookEvent>) {
    if (!webhookURL) {
      logger.info('skipping webhook, no WEBHOOK_URL set')
      return job
    }

    logger.info({ job }, 'handling webhook')

    try {
      await httpClient.post(
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
      logger.error({ error: e }, 'Webhook failed')
      throw e
    }

    return job
  }
}
