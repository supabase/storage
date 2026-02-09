import { HeadBucketCommand, S3Client } from '@aws-sdk/client-s3'
import app from '../admin-app'
import { S3Adapter } from '../storage/backend'
import { Queue } from '@internal/queue'
import { isS3Error } from '@internal/errors'
import path from 'path'

export const adminApp = app({})

const ENV = process.env
const projectRoot = path.join(__dirname, '..', '..')

export function useMockQueue() {
  const queueSpy: jest.SpyInstance | undefined = undefined
  beforeEach(() => {
    mockQueue()
  })

  return queueSpy
}

export function mockQueue() {
  const sendSpy = jest.fn()
  const insertSpy = jest.fn()
  const queueSpy = jest.fn().mockReturnValue({
    send: sendSpy,
    insert: insertSpy,
  })
  jest.spyOn(Queue, 'getInstance').mockImplementation(queueSpy)

  return { queueSpy, sendSpy, insertSpy }
}

export function useMockObject() {
  beforeEach(() => {
    process.env = { ...ENV }

    jest.clearAllMocks()
    jest.spyOn(S3Adapter.prototype, 'read').mockResolvedValue({
      metadata: {
        httpStatusCode: 200,
        size: 3746,
        mimetype: 'image/png',
        lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
        eTag: 'abc',
        cacheControl: 'no-cache',
        contentLength: 3746,
      },
      httpStatusCode: 200,
      body: Buffer.from(''),
    })

    jest.spyOn(S3Adapter.prototype, 'write').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
      cacheControl: 'no-cache',
      contentLength: 3746,
    })

    jest.spyOn(S3Adapter.prototype, 'copy').mockResolvedValue({
      httpStatusCode: 200,
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
    })

    jest.spyOn(S3Adapter.prototype, 'remove').mockResolvedValue()

    jest.spyOn(S3Adapter.prototype, 'removeMany').mockResolvedValue()

    jest.spyOn(S3Adapter.prototype, 'stats').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      eTag: 'abc',
      cacheControl: 'no-cache',
      lastModified: new Date('Wed, 12 Oct 2022 11:17:02 GMT'),
      contentLength: 3746,
    })

    jest
      .spyOn(S3Adapter.prototype, 'tempPrivateAccessUrl')
      .mockResolvedValue(`local:///${projectRoot}/data/sadcat.jpg`)
  })

  afterEach(() => {
    jest.clearAllMocks()
  })
}

export const checkBucketExists = async (client: S3Client, bucket: string) => {
  const options = {
    Bucket: bucket,
  }

  try {
    await client.send(new HeadBucketCommand(options))
    return true
  } catch (error) {
    const err = error as Error

    if (err && isS3Error(err) && err.$metadata.httpStatusCode === 404) {
      return false
    }
    throw error
  }
}
