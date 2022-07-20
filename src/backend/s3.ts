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
import { ObjectMetadata, ObjectResponse } from '../types/types'
import { GenericStorageBackend, GetObjectHeaders } from './generic'
import { convertErrorToStorageBackendError } from '../utils/errors'

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
        cacheControl: data.CacheControl,
        mimetype: data.ContentType,
        eTag: data.ETag,
        lastModified: data.LastModified,
        contentRange: data.ContentRange,
        contentLength: data.ContentLength,
        httpStatusCode: data.$metadata.httpStatusCode,
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
      return {
        httpStatusCode: data.$metadata.httpStatusCode,
      }
    } catch (err: any) {
      throw convertErrorToStorageBackendError(err)
    }
  }

  async deleteObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const command = new DeleteObjectCommand({
      Bucket: bucket,
      Key: key,
    })
    await this.client.send(command)
    return {}
  }

  async copyObject(bucket: string, source: string, destination: string): Promise<ObjectMetadata> {
    const command = new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `/${bucket}/${source}`,
      Key: destination,
    })
    const data = await this.client.send(command)
    return {
      httpStatusCode: data.$metadata.httpStatusCode,
    }
  }

  async deleteObjects(bucket: string, prefixes: string[]): Promise<ObjectMetadata> {
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
    return {}
  }

  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const command = new HeadObjectCommand({
      Bucket: bucket,
      Key: key,
    })
    const data = await this.client.send(command)
    return {
      httpStatusCode: data.$metadata.httpStatusCode,
      size: data.ContentLength,
    }
  }
}
