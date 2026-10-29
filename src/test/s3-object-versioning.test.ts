import {
  CreateBucketCommand,
  GetBucketVersioningCommand,
  PutBucketVersioningCommand,
  S3Client,
  S3ServiceException,
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig, mergeConfig } from '../config'

const { s3ProtocolAccessKeySecret, s3ProtocolAccessKeyId, storageS3Region } = getConfig()

describe('S3 object versioning', () => {
  let testApp: FastifyInstance
  let client: S3Client

  beforeAll(async () => {
    testApp = app()
    const listener = await testApp.listen()
    client = new S3Client({
      endpoint: `${listener.replace('[::1]', 'localhost')}/s3`,
      forcePathStyle: true,
      region: storageS3Region,
      credentials: {
        accessKeyId: s3ProtocolAccessKeyId!,
        secretAccessKey: s3ProtocolAccessKeySecret!,
      },
    })
  })

  beforeEach(() => {
    mergeConfig({ storageVersioningEnabled: true })
  })

  afterEach(() => {
    getConfig({ reload: true })
  })

  afterAll(async () => {
    client.destroy()
    await testApp.close()
  })

  async function createBucket(versioning?: 'Enabled' | 'Suspended') {
    const bucket = `versioning-${randomUUID()}`
    await client.send(new CreateBucketCommand({ Bucket: bucket }))
    if (versioning) {
      await client.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: versioning },
        })
      )
    }
    return bucket
  }

  /** Sends a presigned request so the raw status, headers and body can be asserted. */
  async function raw(signedUrl: Promise<string>, method = 'GET') {
    const response = await fetch(await signedUrl, {
      method,
      headers: { accept: 'application/xml' },
    })
    return {
      status: response.status,
      headers: response.headers,
      body: (await response.text()).replace(/^<\?xml[^>]*\?>/, ''),
    }
  }

  async function sendError(request: Promise<unknown>) {
    return request.then(
      () => {
        throw new Error('expected the request to fail')
      },
      (e: S3ServiceException) => e
    )
  }

  describe('bucket versioning configuration', () => {
    it('reports a never-configured bucket as an empty configuration', async () => {
      const bucket = await createBucket()

      const response = await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))
      expect(response.Status).toBeUndefined()
      expect(response.MFADelete).toBeUndefined()

      const rawResponse = await raw(
        getSignedUrl(client, new GetBucketVersioningCommand({ Bucket: bucket }), { expiresIn: 60 })
      )
      expect(rawResponse.body).toBe(
        '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>'
      )
    })

    it('enables and suspends versioning through PutBucketVersioning', async () => {
      const bucket = await createBucket('Enabled')
      expect((await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))).Status).toBe(
        'Enabled'
      )

      await client.send(
        new PutBucketVersioningCommand({
          Bucket: bucket,
          VersioningConfiguration: { Status: 'Suspended' },
        })
      )
      expect((await client.send(new GetBucketVersioningCommand({ Bucket: bucket }))).Status).toBe(
        'Suspended'
      )
    })

    it('rejects an unknown versioning status as MalformedXML', async () => {
      const bucket = await createBucket()

      const error = await sendError(
        client.send(
          new PutBucketVersioningCommand({
            Bucket: bucket,
            VersioningConfiguration: { Status: 'Disabled' as 'Enabled' },
          })
        )
      )
      expect(error.name).toBe('MalformedXML')
      expect(error.$metadata.httpStatusCode).toBe(400)
    })
  })
})
