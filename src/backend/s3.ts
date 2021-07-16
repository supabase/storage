import {
  CopyObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ObjectIdentifier,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'
import { ObjectMetadata, ObjectResponse } from '../types/types'

export class S3Backend {
  client: S3Client

  constructor(region: string, endpoint?: string | undefined) {
    const params: S3ClientConfig = {
      region,
      runtime: 'node',
      requestHandler: new NodeHttpHandler({
        socketTimeout: 300000,
      }),
    }
    if (endpoint) {
      params.endpoint = endpoint
    }
    this.client = new S3Client(params)
  }

  async getObject(bucketName: string, key: string, range?: string): Promise<ObjectResponse> {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key,
      Range: range,
    })
    const data = await this.client.send(command)
    data.Body
    return {
      metadata: {
        cacheControl: data.CacheControl,
        mimetype: data.ContentType,
        eTag: data.ETag,
        lastModified: data.LastModified,
        contentRange: data.ContentRange,
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

  async deleteObjects(bucket: string, prefixes: ObjectIdentifier[]): Promise<ObjectMetadata> {
    const command = new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: {
        Objects: prefixes,
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
