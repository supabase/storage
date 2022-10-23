import {
  CopyObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadObjectCommand,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'
import https from 'https'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'
import { GenericStorageBackend, GetObjectHeaders, ObjectMetadata, ObjectResponse } from './generic'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { StorageBackendError } from '../errors'

export class S3Backend implements GenericStorageBackend {
  client: S3Client

  constructor(region: string, endpoint?: string | undefined) {
    const agent = new https.Agent({
      maxSockets: 50,
      keepAlive: true,
    })
    const params: S3ClientConfig = {
      region,
      runtime: 'node',
      requestHandler: new NodeHttpHandler({
        httpsAgent: agent,
        socketTimeout: 3000,
      }),
    }
    if (endpoint) {
      params.endpoint = endpoint
    }
    this.client = new S3Client(params)
  }

  async getObject(
    bucketName: string,
    key: string,
    headers?: GetObjectHeaders
  ): Promise<ObjectResponse> {
    const input: GetObjectCommandInput = {
      Bucket: bucketName,
      IfNoneMatch: headers?.ifNoneMatch,
      Key: key,
      Range: headers?.range,
    }
    if (headers?.ifModifiedSince) {
      input.IfModifiedSince = new Date(headers.ifModifiedSince)
    }
    const command = new GetObjectCommand(input)
    const data = await this.client.send(command)

    return {
      metadata: {
        cacheControl: data.CacheControl || 'no-cache',
        mimetype: data.ContentType || 'application/octa-stream',
        eTag: data.ETag || '',
        lastModified: data.LastModified,
        contentRange: data.ContentRange,
        contentLength: data.ContentLength || 0,
        httpStatusCode: data.$metadata.httpStatusCode || 200,
        size: data.ContentLength || 0,
      },
      body: data.Body,
    }
  }

  async uploadObject(
    bucketName: string,
    key: string,
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    try {
      const paralellUploadS3 = new Upload({
        client: this.client,
        params: {
          Bucket: bucketName,
          Key: key,
          /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
          Body: body,
          ContentType: contentType,
          CacheControl: cacheControl,
        },
      })

      const data = await paralellUploadS3.done()
      const metadata = await this.headObject(bucketName, key)

      return {
        httpStatusCode: data.$metadata.httpStatusCode || metadata.httpStatusCode,
        cacheControl: metadata.cacheControl,
        eTag: metadata.eTag,
        mimetype: metadata.mimetype,
        contentLength: metadata.contentLength,
        lastModified: metadata.lastModified,
        size: metadata.size,
        contentRange: metadata.contentRange,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async deleteObject(bucket: string, key: string): Promise<void> {
    const command = new DeleteObjectCommand({
      Bucket: bucket,
      Key: key,
    })
    await this.client.send(command)
  }

  async copyObject(
    bucket: string,
    source: string,
    destination: string
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
    const command = new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `/${bucket}/${source}`,
      Key: destination,
    })
    const data = await this.client.send(command)
    return {
      httpStatusCode: data.$metadata.httpStatusCode || 200,
    }
  }

  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    const s3Prefixes = prefixes.map((ele) => {
      return { Key: ele }
    })

    const command = new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: {
        Objects: s3Prefixes,
      },
    })
    await this.client.send(command)
  }

  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const command = new HeadObjectCommand({
      Bucket: bucket,
      Key: key,
    })
    const data = await this.client.send(command)
    return {
      cacheControl: data.CacheControl || 'no-cache',
      mimetype: data.ContentType || 'application/octet-stream',
      eTag: data.ETag || '',
      lastModified: data.LastModified,
      contentLength: data.ContentLength || 0,
      httpStatusCode: data.$metadata.httpStatusCode || 200,
      size: data.ContentLength || 0,
    }
  }

  async privateAssetUrl(bucket: string, key: string): Promise<string> {
    const input: GetObjectCommandInput = {
      Bucket: bucket,
      Key: key,
    }

    const command = new GetObjectCommand(input)
    return getSignedUrl(this.client, command, { expiresIn: 300 })
  }
}
