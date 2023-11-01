import {
  CompleteMultipartUploadCommandOutput,
  CopyObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadObjectCommand,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
} from './generic'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { StorageBackendError } from '../errors'
import { getConfig } from '../../config'
import Agent, { HttpsAgent } from 'agentkeepalive'

const { globalS3Protocol, globalS3MaxSockets } = getConfig()

/**
 * Creates an agent for the given protocol
 * @param protocol
 */
export function createAgent(protocol: 'http' | 'https') {
  const agentOptions = {
    maxSockets: globalS3MaxSockets,
    keepAlive: true,
  }

  return protocol === 'http'
    ? { httpAgent: new Agent(agentOptions) }
    : { httpsAgent: new HttpsAgent(agentOptions) }
}

export interface S3ClientOptions {
  endpoint?: string
  region?: string
  forcePathStyle?: boolean
  accessKey?: string
  secretKey?: string
  role?: string
  httpAgent?: { httpAgent: Agent } | { httpsAgent: HttpsAgent }
}

/**
 * S3Backend
 * Interacts with an s3-compatible file system with this S3Adapter
 */
export class S3Backend implements StorageBackendAdapter {
  client: S3Client

  constructor(options: S3ClientOptions) {
    const agent = options.httpAgent ? options.httpAgent : createAgent(globalS3Protocol)

    const params: S3ClientConfig = {
      region: options.region,
      runtime: 'node',
      requestHandler: new NodeHttpHandler({
        ...agent,
      }),
    }
    if (options.endpoint) {
      params.endpoint = options.endpoint
    }
    if (options.forcePathStyle) {
      params.forcePathStyle = true
    }
    this.client = new S3Client(params)
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param version
   * @param headers
   */
  async getObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders
  ): Promise<ObjectResponse> {
    const input: GetObjectCommandInput = {
      Bucket: bucketName,
      IfNoneMatch: headers?.ifNoneMatch,
      Key: withOptionalVersion(key, version),
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

  /**
   * Uploads and store an object
   * @param bucketName
   * @param key
   * @param version
   * @param body
   * @param contentType
   * @param cacheControl
   */
  async uploadObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    try {
      const paralellUploadS3 = new Upload({
        client: this.client,
        params: {
          Bucket: bucketName,
          Key: withOptionalVersion(key, version),
          /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
          Body: body,
          ContentType: contentType,
          CacheControl: cacheControl,
        },
      })

      const data = (await paralellUploadS3.done()) as CompleteMultipartUploadCommandOutput

      const metadata = await this.headObject(bucketName, key, version)

      return {
        httpStatusCode: data.$metadata.httpStatusCode || metadata.httpStatusCode,
        cacheControl: cacheControl,
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

  async setMetadataToCompleted(bucketName: string, key: string) {
    const headObject = new HeadObjectCommand({
      Bucket: bucketName,
      Key: `${key}.info`,
    })
    const findObjResp = await this.client.send(headObject)

    const copyCmd = new CopyObjectCommand({
      Bucket: bucketName,
      CopySource: `${bucketName}/${key}.info`,
      Key: `${key}.info`,
      Metadata: {
        ...findObjResp.Metadata,
        tus_completed: 'true',
      },
      MetadataDirective: 'REPLACE',
    })

    return this.client.send(copyCmd)
  }

  /**
   * Deletes an object
   * @param bucket
   * @param key
   * @param version
   */
  async deleteObject(bucket: string, key: string, version: string | undefined): Promise<void> {
    const command = new DeleteObjectCommand({
      Bucket: bucket,
      Key: withOptionalVersion(key, version),
    })
    await this.client.send(command)
  }

  /**
   * Copies an existing object to the given location
   * @param bucket
   * @param source
   * @param version
   * @param destination
   * @param destinationVersion
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
    try {
      const command = new CopyObjectCommand({
        Bucket: bucket,
        CopySource: `${bucket}/${withOptionalVersion(source, version)}`,
        Key: withOptionalVersion(destination, destinationVersion),
      })
      const data = await this.client.send(command)
      return {
        httpStatusCode: data.$metadata.httpStatusCode || 200,
      }
    } catch (e: any) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Deletes multiple objects
   * @param bucket
   * @param prefixes
   */
  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    try {
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
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns metadata information of a specific object
   * @param bucket
   * @param key
   * @param version
   */
  async headObject(
    bucket: string,
    key: string,
    version: string | undefined
  ): Promise<ObjectMetadata> {
    try {
      const command = new HeadObjectCommand({
        Bucket: bucket,
        Key: withOptionalVersion(key, version),
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
    } catch (e: any) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   * @param version
   */
  async privateAssetUrl(bucket: string, key: string, version: string | undefined): Promise<string> {
    const input: GetObjectCommandInput = {
      Bucket: bucket,
      Key: withOptionalVersion(key, version),
    }

    const command = new GetObjectCommand(input)
    return getSignedUrl(this.client, command, { expiresIn: 600 })
  }
}
