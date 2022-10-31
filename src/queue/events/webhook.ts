import { BaseEvent } from './base-event'
import { Job, WorkOptions } from 'pg-boss'
import axios from 'axios'
import { getConfig } from '../../config'
import { logger } from '../../monitoring'
import apikey from '../../http/plugins/apikey'

const { webhookURL, webhookApiKey } = getConfig()

interface WebhookEvent {
  eventName: string
  payload: object
  sentAt: string
  applyTime: number
}

export class Webhook extends BaseEvent<WebhookEvent> {
  static queueName = 'webhooks'

  static getWorkerOptions(): WorkOptions {
    return {
      // batchSize: 30,
      newJobCheckIntervalSeconds: 2,
    }
  }

  static async handle(job: Job<WebhookEvent>) {
    console.log('WEBHOOK JOB', webhookURL)
    if (!webhookURL) {
      logger.info('skipping webhook, no WEBHOOK_URL set')
      return job
    }

    try {
      const response = await axios.post(
        webhookURL,
        {
          type: job.data.eventName,
          payload: job.data.payload,
          sentAt: new Date(),
          applyTime: job.data.applyTime,
        },
        {
          headers: {
            ...(webhookApiKey ? { authorization: `Bearer ${apikey}` } : {}),
          },
        }
      )

      console.log(response)
    } catch (e) {
      console.error(e)
      throw e
    }

    return job
  }
}
