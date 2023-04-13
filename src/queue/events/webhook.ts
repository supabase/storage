import { BaseEvent } from './base-event'
import { Job, WorkOptions } from 'pg-boss'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger } from '../../monitoring'
import { Queue } from '../queue'

const { webhookURL, webhookApiKey, webhookQueuePullInterval } = getConfig()

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
      batchSize: 100,
    }
  }

  static async handle(jobs: Job<WebhookEvent>[]) {
    if (!webhookURL) {
      try {
        await Queue.getInstance().complete(jobs.map((job) => job.id))
      } catch (e) {
        logger.error({ error: e }, 'Webhook failed')
        throw e
      }

      logger.info('skipping webhook, no WEBHOOK_URL set')

      return jobs
    }

    const promises = jobs.map(async (job) => {
      logger.info({ job }, 'handling webhook')

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

        await Queue.getInstance().complete(job.id)
      } catch (e) {
        await Queue.getInstance().fail(job.id)
        logger.error({ error: e }, 'Webhook failed')
      }

      return job
    })

    return Promise.all(promises)
  }
}
