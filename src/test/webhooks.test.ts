process.env.ENABLE_QUEUE_EVENTS = 'true'

import { mockQueue, useMockObject } from './common'
import FormData from 'form-data'

import fs from 'fs'
import app from '../app'
import { getConfig } from '../config'
import { getPostgrestClient } from '../database'
import { PostgrestClient } from '@supabase/postgrest-js'
import { Obj } from '../storage/schemas'

const { serviceKey } = getConfig()

describe('Webhooks', () => {
  useMockObject()

  let pg: PostgrestClient
  beforeAll(async () => {
    pg = await getPostgrestClient(serviceKey, {})
  })

  let sendSpy: jest.SpyInstance
  beforeEach(() => {
    const mocks = mockQueue()
    sendSpy = mocks.sendSpy
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('will emit a webhook upon object creation', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket6/public/test-33.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(sendSpy).toBeCalledTimes(1)
    expect(sendSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: expect.objectContaining({
            type: 'ObjectCreated:Put',
            $version: 'v1',
            applyTime: expect.any(Number),
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              metadata: expect.objectContaining({
                cacheControl: 'no-cache',
                contentLength: 3746,
                eTag: 'abc',
                lastModified: expect.any(Date),
                httpStatusCode: 200,
                mimetype: 'image/png',
                size: 3746,
              }),
              name: 'public/test-33.png',
              tenant: expect.objectContaining({
                ref: 'bjhaohmqunupljrqypxz',
              }),
            }),
          }),
          tenant: expect.objectContaining({
            ref: 'bjhaohmqunupljrqypxz',
          }),
        }),
      })
    )
  })

  it('will emit a webhook upon object deletion', async () => {
    const obj = await createObject(pg, 'bucket6')

    const response = await app().inject({
      method: 'DELETE',
      url: `/object/bucket6/${obj.name}`,
      headers: {
        Authorization: `Bearer ${serviceKey}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(sendSpy).toBeCalledTimes(1)
    expect(sendSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: expect.objectContaining({
            $version: 'v1',
            type: 'ObjectRemoved:Delete',
            applyTime: expect.any(Number),
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              name: obj.name,
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
            }),
          }),
          tenant: {
            host: undefined,
            ref: 'bjhaohmqunupljrqypxz',
          },
        }),
      })
    )
  })

  it('will emit a webhook upon object moved', async () => {
    const obj = await createObject(pg, 'bucket6')

    const response = await app().inject({
      method: 'POST',
      url: `/object/move`,
      headers: {
        Authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        bucketId: 'bucket6',
        sourceKey: obj.name,
        destinationKey: `${obj.name}-moved`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(sendSpy).toBeCalledTimes(2)

    expect(sendSpy).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: expect.objectContaining({
            $version: 'v1',
            type: 'ObjectRemoved:Move',
            applyTime: expect.any(Number),
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              name: obj.name,
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
            }),
          }),
          tenant: {
            host: undefined,
            ref: 'bjhaohmqunupljrqypxz',
          },
        }),
      })
    )

    expect(sendSpy).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: {
            $version: 'v1',
            type: 'ObjectCreated:Move',
            applyTime: expect.any(Number),
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              metadata: expect.objectContaining({
                cacheControl: 'no-cache',
                contentLength: 3746,
                eTag: 'abc',
                lastModified: expect.any(String),
                httpStatusCode: 200,
                mimetype: 'image/png',
                size: 3746,
              }),
              name: `${obj.name}-moved`,
              oldObject: {
                bucketId: 'bucket6',
                name: obj.name,
              },
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
            }),
          },
          tenant: {
            host: undefined,
            ref: 'bjhaohmqunupljrqypxz',
          },
        }),
      })
    )
  })

  it('will emit a webhook upon object copied', async () => {
    const obj = await createObject(pg, 'bucket6')

    const response = await app().inject({
      method: 'POST',
      url: `/object/copy`,
      headers: {
        Authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        bucketId: 'bucket6',
        sourceKey: obj.name,
        destinationKey: `${obj.name}-copied`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(sendSpy).toBeCalledTimes(1)

    expect(sendSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: expect.objectContaining({
            $version: 'v1',
            applyTime: expect.any(Number),
            type: 'ObjectCreated:Copy',
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              metadata: expect.objectContaining({
                cacheControl: 'no-cache',
                contentLength: 3746,
                eTag: 'abc',
                lastModified: expect.any(String),
                httpStatusCode: 200,
                mimetype: 'image/png',
                size: 3746,
              }),
              name: `${obj.name}-copied`,
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
            }),
          }),
          tenant: {
            host: undefined,
            ref: 'bjhaohmqunupljrqypxz',
          },
        }),
      })
    )
  })
})

async function createObject(pg: PostgrestClient, bucketId: string) {
  const objectName = Date.now()
  const { data, error } = await pg
    .from<Obj>('objects')
    .insert([
      {
        name: objectName.toString(),
        bucket_id: bucketId,
        metadata: {
          cacheControl: 'no-cache',
          contentLength: 3746,
          eTag: 'abc',
          lastModified: new Date(),
          httpStatusCode: 200,
          mimetype: 'image/png',
          size: 3746,
        },
      },
    ])
    .single()

  if (error) {
    console.error(error)
    throw error
  }

  return data as Obj
}
