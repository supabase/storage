import { getConfig, mergeConfig } from '../config'

// `webhooksEnabled` is read once, at module load, by src/storage/events/webhooks/webhook.ts —
// so disabling it has to happen before any module in that graph is first imported
vi.hoisted(() => {
  process.env.PG_QUEUE_ENABLE = 'true'
  process.env.WEBHOOK_URL = 'https://example.com/webhook'
  process.env.WEBHOOKS_ENABLED = 'false'
})

const { serviceKeyAsync } = getConfig()

mergeConfig({
  pgQueueEnable: true,
  webhooksEnabled: false,
})

import { randomUUID } from 'crypto'
import type { FastifyInstance } from 'fastify'
import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { Webhook } from '../storage/events'
import { mockQueue, useMockObject } from './common'

describe('Webhooks disabled (WEBHOOKS_ENABLED=false)', () => {
  useMockObject()

  let appInstance: FastifyInstance
  let sendSpy: ReturnType<typeof mockQueue>['sendSpy']
  beforeEach(() => {
    sendSpy = mockQueue().sendSpy
    appInstance = app()
  })

  afterEach(async () => {
    await appInstance.close()
  })

  it('still logs the lifecycle event, but never enqueues a webhook, on object creation', async () => {
    const form = new FormData()
    const authorization = `Bearer ${await serviceKeyAsync}`
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const fileName = randomUUID()

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/bucket6/public/${fileName}.png`,
      headers: Object.assign({}, form.getHeaders(), { authorization }),
      payload: form,
    })

    expect(response.statusCode, response.body).toBe(200)
    expect(sendSpy.mock.calls.some(([message]) => message instanceof Webhook)).toBe(false)
  })
})
