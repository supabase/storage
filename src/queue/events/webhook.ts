import { BaseEvent } from './base-event'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger } from '../../monitoring'
import apikey from '../../http/plugins/apikey'

const { webhookURL, webhookApiKey } = getConfig()

interface WebhookEvent {
  eventName: string
  payload: object
}

export class Webhook extends BaseEvent<WebhookEvent> {
  static queueName = 'webhooks'

  static getWorkerOptions(): WorkOptions | undefined {
    return {
      batchSize: 30,
      newJobCheckIntervalSeconds: 10,
    }
  }

  static async handle(job: Job<WebhookEvent>) {
    if (!webhookURL) {
      logger.info('skipping webhook, no WEBHOOK_URL set')
      return
    }

    const client = axios.create({
      baseURL: webhookURL,
      headers: {
        ...(webhookApiKey ? { authorization: `Bearer ${apikey}` } : {}),
      },
    })

    await client.post('/', {
      type: job.data.eventName,
      payload: job.data.payload,
      sentAt: new Date(),
    })
  }
}
