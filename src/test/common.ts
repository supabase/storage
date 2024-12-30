import { HeadBucketCommand, S3Client } from '@aws-sdk/client-s3'
import app from '../admin-app'
import { S3Backend } from '../storage/backend'
import { Queue } from '@internal/queue'
import { isS3Error } from '@internal/errors'

export const adminApp = app({})

const ENV = process.env

/**
 * Should support all Unicode characters with UTF-8 encoding according to AWS S3 object naming guide, including:
 * - Safe characters: 0-9 a-z A-Z !-_.*'()
 * - Characters that might require special handling: &$@=;/:+,? and Space and ASCII characters \t, \n, and \r.
 * - Characters: \{}^%`[]"<>~#| and non-printable ASCII characters (128–255 decimal characters).
 *
 * The following characters are not allowed:
 * - ASCII characters 0x00–0x1F, except 0x09, 0x0A, and 0x0D.
 * - Unicode \u{FFFE} and \u{FFFF}.
 * - Lone surrogates characters.
 * See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
 * See: https://www.w3.org/TR/REC-xml/#charsets
 */
export function getUnicodeObjectName(): string {
  const objectName = 'test'
    .concat("!-_*.'()")
    // Characters that might require special handling
    .concat('&$@=;:+,? \x09\x0A\x0D')
    // Characters to avoid
    .concat('\\{}^%`[]"<>~#|\xFF')
    // MinIO max. length for each '/' separated segment is 255
    .concat('/')
    .concat([...Array(127).keys()].map((i) => String.fromCodePoint(i + 128)).join(''))
    .concat('/')
    // Some special Unicode characters
    .concat('\u2028\u202F\u{0001FFFF}')
    // Some other Unicode characters
    .concat('일이삼\u{0001f642}')

  return objectName
}

export function getInvalidObjectName(): string {
  return 'test\x01\x02\x03.txt'
}

export function useMockQueue() {
  const queueSpy: jest.SpyInstance | undefined = undefined
  beforeEach(() => {
    mockQueue()
  })

  return queueSpy
}

export function mockQueue() {
  const sendSpy = jest.fn()
  const queueSpy = jest.fn().mockReturnValue({
    send: sendSpy,
  })
  jest.spyOn(Queue, 'getInstance').mockImplementation(queueSpy as any)

  return { queueSpy, sendSpy }
}

export function useMockObject() {
  beforeEach(() => {
    process.env = { ...ENV }

    jest.clearAllMocks()
    jest.spyOn(S3Backend.prototype, 'getObject').mockResolvedValue({
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

    jest.spyOn(S3Backend.prototype, 'uploadObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
      cacheControl: 'no-cache',
      contentLength: 3746,
    })

    jest.spyOn(S3Backend.prototype, 'copyObject').mockResolvedValue({
      httpStatusCode: 200,
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
    })

    jest.spyOn(S3Backend.prototype, 'deleteObject').mockResolvedValue()

    jest.spyOn(S3Backend.prototype, 'deleteObjects').mockResolvedValue()

    jest.spyOn(S3Backend.prototype, 'headObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      eTag: 'abc',
      cacheControl: 'no-cache',
      lastModified: new Date('Wed, 12 Oct 2022 11:17:02 GMT'),
      contentLength: 3746,
    })

    jest.spyOn(S3Backend.prototype, 'privateAssetUrl').mockResolvedValue('local:///data/sadcat.jpg')
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
