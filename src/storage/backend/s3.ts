import {
  CompleteMultipartUploadCommand,
  CompleteMultipartUploadCommandOutput,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  ListPartsCommand,
  PutObjectCommand,
  S3Client,
  S3ClientConfig,
  UploadPartCommand,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
} from './generic'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { StorageBackendError } from '../errors'
import { getConfig } from '../../config'
import Agent, { HttpsAgent } from 'agentkeepalive'
import stream, { Readable } from 'stream'

const { storageS3MaxSockets } = getConfig()

/**
 * Creates an agent for the given protocol
 * @param protocol
 */
export function createAgent(protocol: 'http' | 'https') {
  const agentOptions = {
    maxSockets: storageS3MaxSockets,
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
    const storageS3Protocol = options.endpoint?.includes('http://') ? 'http' : 'https'
    const agent = options.httpAgent ? options.httpAgent : createAgent(storageS3Protocol)

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

  async createMultiPartUpload(
    bucketName: string,
    key: string,
    version: string | undefined,
    contentType: string,
    cacheControl: string
  ) {
    const createMultiPart = new CreateMultipartUploadCommand({
      Bucket: bucketName,
      Key: withOptionalVersion(key, version),
      CacheControl: cacheControl,
      ContentType: contentType,
      Metadata: {
        Version: version || '',
      },
    })
    const resp = await this.client.send(createMultiPart)

    const uploadInfo = new PutObjectCommand({
      Bucket: bucketName,
      Key: `.info.${withOptionalVersion(key)}/${resp.UploadId}`,
      Metadata: {
        Version: version || '',
      },
    })
    await this.client.send(uploadInfo)

    return resp.UploadId
  }

  async uploadPart(
    bucketName: string,
    key: string,
    uploadId: string,
    partNumber: number,
    body?: string | Uint8Array | Buffer | Readable,
    length?: number
  ) {
    const uploadInfo = new HeadObjectCommand({
      Bucket: bucketName,
      Key: `.info.${withOptionalVersion(key)}/${uploadId}`,
    })
    const objMapping = await this.client.send(uploadInfo)

    if (!objMapping) {
      throw new Error('Upload ID not found')
    }

    const version = objMapping.Metadata?.version

    if (!version) {
      throw new Error('missing version metadata')
    }

    const paralellUploadS3 = new UploadPartCommand({
      Bucket: bucketName,
      Key: `${key}/${objMapping.Metadata?.version}`,
      UploadId: uploadId,
      PartNumber: partNumber,
      Body: body,
      ContentLength: length,
    })

    const resp = await this.client.send(paralellUploadS3)

    return {
      // partNumber: resp.
      version,
      ETag: resp.ETag,
    }
  }

  async completeMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    parts: UploadPart[]
  ) {
    const uploadInfo = new HeadObjectCommand({
      Bucket: bucketName,
      Key: `.info.${key}/${uploadId}`,
    })
    const objMapping = await this.client.send(uploadInfo)

    if (!objMapping) {
      throw new Error('Upload ID not found')
    }

    const version = objMapping.Metadata?.version

    if (!version) {
      throw new Error('missing version metadata')
    }

    if (parts.length === 0) {
      const listPartsInput = new ListPartsCommand({
        Bucket: bucketName,
        Key: key + '/' + version,
        UploadId: uploadId,
      })

      const partsResponse = await this.client.send(listPartsInput)
      parts = partsResponse.Parts || []
    }

    const completeUpload = new CompleteMultipartUploadCommand({
      Bucket: bucketName,
      Key: key + '/' + version,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: parts,
      },
    })

    const response = await this.client.send(completeUpload)

    const keyParts = key.split('/')
    const tenantId = keyParts.shift()
    const bucket = keyParts.shift()

    // remove object version from key
    // const removeObject = new ObjectDeleteCommand()
    // this.client.send()

    return {
      version,
      location: keyParts.join('/'),
      bucket,
      ...response,
    }
  }
}
