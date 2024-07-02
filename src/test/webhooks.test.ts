import { TenantConnection } from '@internal/database'
import { getConfig, mergeConfig } from '../config'

const { serviceKey, tenantId } = getConfig()

mergeConfig({
  pgQueueEnable: true,
})

import { mockQueue, useMockObject } from './common'
import FormData from 'form-data'

import fs from 'fs'
import app from '../app'
import { getPostgresConnection } from '@internal/database'
import { Obj } from '@storage/schemas'
import { randomUUID } from 'crypto'
import { getServiceKeyUser } from '@internal/database'

describe('Webhooks', () => {
  useMockObject()

  let pg: TenantConnection
  beforeAll(async () => {
    const superUser = await getServiceKeyUser(tenantId)
    pg = await getPostgresConnection({
      tenantId,
      superUser,
      user: superUser,
      host: 'localhost',
    })
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
    })

    const fileName = (Math.random() + 1).toString(36).substring(7)

    const response = await app().inject({
      method: 'POST',
      url: `/object/bucket6/public/${fileName}.png`,
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
            type: 'ObjectCreated:Post',
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
              name: `public/${fileName}.png`,
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

    expect(sendSpy).toHaveBeenNthCalledWith(
      1,
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
              reqId: expect.any(String),
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
    expect(sendSpy).toBeCalledTimes(3)

    expect(sendSpy).toHaveBeenNthCalledWith(
      2,
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
              version: expect.any(String),
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
              reqId: expect.any(String),
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
      3,
      expect.objectContaining({
        name: 'webhooks',
        options: undefined,
        data: expect.objectContaining({
          $version: 'v1',
          event: expect.objectContaining({
            $version: 'v1',
            type: 'ObjectCreated:Move',
            applyTime: expect.any(Number),
            payload: expect.objectContaining({
              bucketId: 'bucket6',
              version: expect.any(String),
              metadata: expect.objectContaining({
                cacheControl: 'no-cache',
                contentLength: 3746,
                eTag: 'abc',
                lastModified: expect.any(Date),
                httpStatusCode: 200,
                mimetype: 'image/png',
                size: 3746,
              }),
              name: `${obj.name}-moved`,
              oldObject: {
                bucketId: 'bucket6',
                name: obj.name,
                reqId: expect.any(String),
                version: expect.any(String),
              },
              tenant: {
                host: undefined,
                ref: 'bjhaohmqunupljrqypxz',
              },
              reqId: expect.any(String),
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
                lastModified: expect.any(Date),
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

async function createObject(pg: TenantConnection, bucketId: string) {
  const objectName = Date.now()
  const tnx = await pg.transaction()

  const [data] = await tnx
    .from<Obj>('objects')
    .insert([
      {
        name: objectName.toString(),
        bucket_id: bucketId,
        version: randomUUID(),
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
    .returning('*')

  await tnx.commit()

  return data as Obj
}
