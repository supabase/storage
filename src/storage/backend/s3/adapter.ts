import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListPartsCommand,
  S3Client,
  S3ClientConfig,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { Progress, Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
} from './../adapter'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../../config'
import { Readable } from 'node:stream'
import { createAgent, InstrumentedAgent } from '@internal/http'
import { monitorStream } from '@internal/streams'
import { BackupObjectInfo, ObjectBackup } from '@storage/backend/s3/backup'

const { storageS3UploadQueueSize, tracingFeatures, storageS3MaxSockets, tracingEnabled } =
  getConfig()

export interface S3ClientOptions {
  endpoint?: string
  region?: string
  forcePathStyle?: boolean
  accessKey?: string
  secretKey?: string
  role?: string
  httpAgent?: InstrumentedAgent
  requestTimeout?: number
}

/**
 * S3Backend
 * Interacts with a s3-compatible file system with this S3Adapter
 */
export class S3Backend implements StorageBackendAdapter {
  client: S3Client
  agent: InstrumentedAgent

  constructor(options: S3ClientOptions) {
    this.agent =
      options.httpAgent ??
      createAgent('s3_default', {
        maxSockets: storageS3MaxSockets,
      })

    if (this.agent.httpsAgent && tracingEnabled) {
      this.agent.monitor()
    }

    // Default client for API operations
    this.client = this.createS3Client({
      ...options,
      name: 's3_default',
      httpAgent: this.agent,
    })
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param version
   * @param headers
   * @param signal
   */
  async getObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders,
    signal?: AbortSignal
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
    const data = await this.client.send(command, {
      abortSignal: signal,
    })

    return {
      metadata: {
        cacheControl: data.CacheControl || 'no-cache',
        mimetype: data.ContentType || 'application/octa-stream',
        eTag: data.ETag || '',
        lastModified: data.LastModified,
        contentRange: data.ContentRange,
        contentLength: data.ContentLength || 0,
        size: data.ContentLength || 0,
        httpStatusCode: data.$metadata.httpStatusCode || 200,
      },
      httpStatusCode: data.$metadata.httpStatusCode || 200,
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
   * @param signal
   */
  async uploadObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    body: Readable,
    contentType: string,
    cacheControl: string,
    signal?: AbortSignal
  ): Promise<ObjectMetadata> {
    if (signal?.aborted) {
      throw ERRORS.Aborted('Upload was aborted')
    }

    const dataStream = tracingFeatures?.upload ? monitorStream(body) : body

    const upload = new Upload({
      client: this.client,
      queueSize: storageS3UploadQueueSize,
      params: {
        Bucket: bucketName,
        Key: withOptionalVersion(key, version),
        Body: dataStream,
        ContentType: contentType,
        CacheControl: cacheControl,
      },
    })

    signal?.addEventListener('abort', () => upload.abort(), { once: true })

    let hasUploadedBytes = false
    const progressHandler = (progress: Progress) => {
      if (!hasUploadedBytes && progress.loaded && progress.loaded > 0) {
        hasUploadedBytes = true
      }
      if (tracingFeatures?.upload) {
        dataStream.emit('s3_progress', JSON.stringify(progress))
      }
    }
    upload.on('httpUploadProgress', progressHandler)

    try {
      const data = await upload.done()

      // Remove event listener to allow GC of upload and dataStream references
      upload.off('httpUploadProgress', progressHandler)

      // Only call head for objects that are > 0 bytes
      // for some reason headObject can take a really long time to resolve on zero byte uploads, this was causing requests to timeout
      const metadata = hasUploadedBytes
        ? await this.headObject(bucketName, key, version)
        : {
            httpStatusCode: 200,
            eTag: data.ETag || '',
            mimetype: contentType,
            lastModified: new Date(),
            size: 0,
            contentLength: 0,
            contentRange: undefined,
          }

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
    } catch (err) {
      upload.off('httpUploadProgress', progressHandler)

      if (err instanceof Error && err.name === 'AbortError') {
        throw ERRORS.AbortedTerminate('Upload was aborted', err)
      }
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
   * @param metadata
   * @param conditions
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined,
    metadata?: { cacheControl?: string; mimetype?: string },
    conditions?: {
      ifMatch?: string
      ifNoneMatch?: string
      ifModifiedSince?: Date
      ifUnmodifiedSince?: Date
    }
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    try {
      const command = new CopyObjectCommand({
        Bucket: bucket,
        CopySource: encodeURIComponent(`${bucket}/${withOptionalVersion(source, version)}`),
        Key: withOptionalVersion(destination, destinationVersion),
        CopySourceIfMatch: conditions?.ifMatch,
        CopySourceIfNoneMatch: conditions?.ifNoneMatch,
        CopySourceIfModifiedSince: conditions?.ifModifiedSince,
        CopySourceIfUnmodifiedSince: conditions?.ifUnmodifiedSince,
        ContentType: metadata?.mimetype,
        CacheControl: metadata?.cacheControl,
      })
      const data = await this.client.send(command)
      return {
        httpStatusCode: data.$metadata.httpStatusCode || 200,
        eTag: data.CopyObjectResult?.ETag || '',
        lastModified: data.CopyObjectResult?.LastModified,
      }
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  async list(
    bucket: string,
    options?: {
      prefix?: string
      delimiter?: string
      nextToken?: string
      startAfter?: string
      beforeDate?: Date
    }
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    try {
      const command = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: options?.prefix,
        Delimiter: options?.delimiter,
        ContinuationToken: options?.nextToken || undefined,
        StartAfter: options?.startAfter,
      })
      const data = await this.client.send(command)
      const keys =
        data.Contents?.filter((ele) => {
          if (options?.beforeDate) {
            if (ele.LastModified && ele.LastModified < options.beforeDate) {
              return ele.Key as string
            }
            return false
          }
          return ele.Key
        }).map((ele) => {
          if (options?.prefix) {
            return {
              // remove prefix and leading slash if present
              name: (ele.Key as string).replace(options.prefix, '').replace(/^\//, ''),
              size: ele.Size as number,
            }
          }

          return { name: ele.Key as string, size: ele.Size as number }
        }) || []

      return {
        keys,
        nextToken: data.NextContinuationToken,
      }
    } catch (e) {
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
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  async listParts(
    bucket: string,
    key: string,
    uploadId?: string,
    maxParts?: number,
    marker?: string
  ) {
    try {
      const command = new ListPartsCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        PartNumberMarker: marker,
        MaxParts: maxParts,
      })

      const result = await this.client.send(command)

      return {
        parts: result.Parts || [],
        nextPartNumberMarker: result.NextPartNumberMarker,
        isTruncated: result.IsTruncated || false,
        httpStatusCode: result.$metadata.httpStatusCode || 200,
      }
    } catch (e) {
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
    cacheControl: string,
    metadata?: Record<string, string>
  ) {
    const createMultiPart = new CreateMultipartUploadCommand({
      Bucket: bucketName,
      Key: withOptionalVersion(key, version),
      CacheControl: cacheControl,
      ContentType: contentType,
      Metadata: metadata
        ? {
            ...metadata,
            Version: version || '',
          }
        : undefined,
    })

    const resp = await this.client.send(createMultiPart)

    if (!resp.UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    return resp.UploadId
  }

  async uploadPart(
    bucketName: string,
    key: string,
    version: string,
    uploadId: string,
    partNumber: number,
    body?: string | Uint8Array | Buffer | Readable,
    length?: number,
    signal?: AbortSignal
  ) {
    try {
      const paralellUploadS3 = new UploadPartCommand({
        Bucket: bucketName,
        Key: version ? `${key}/${version}` : key,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: body,
        ContentLength: length,
      })

      const resp = await this.client.send(paralellUploadS3, {
        abortSignal: signal,
      })

      return {
        version,
        ETag: resp.ETag,
      }
    } catch (e) {
      if (e instanceof Error && e.name === 'AbortError') {
        throw ERRORS.AbortedTerminate('Upload was aborted', e)
      }

      throw StorageBackendError.fromError(e)
    }
  }

  async completeMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    version: string,
    parts: UploadPart[],
    opts?: { removePrefix?: boolean }
  ) {
    const keyParts = key.split('/')

    if (parts.length === 0) {
      const listPartsInput = new ListPartsCommand({
        Bucket: bucketName,
        Key: version ? key + '/' + version : key,
        UploadId: uploadId,
      })

      const partsResponse = await this.client.send(listPartsInput)
      parts = partsResponse.Parts || []
    }

    const completeUpload = new CompleteMultipartUploadCommand({
      Bucket: bucketName,
      Key: version ? key + '/' + version : key,
      UploadId: uploadId,
      MultipartUpload:
        parts.length === 0
          ? undefined
          : {
              Parts: parts,
            },
    })

    const response = await this.client.send(completeUpload)

    let location = key
    let bucket = bucketName

    if (opts?.removePrefix) {
      const locationParts = key.split('/')
      locationParts.shift() // tenant-id

      bucket = keyParts.shift() || ''
      location = keyParts.join('/')
    }

    return {
      version,
      location: location,
      bucket,
      ...response,
    }
  }

  async abortMultipartUpload(bucketName: string, key: string, uploadId: string): Promise<void> {
    const abortUpload = new AbortMultipartUploadCommand({
      Bucket: bucketName,
      Key: key,
      UploadId: uploadId,
    })
    await this.client.send(abortUpload)
  }

  async uploadPartCopy(
    storageS3Bucket: string,
    key: string,
    version: string,
    UploadId: string,
    PartNumber: number,
    sourceKey: string,
    sourceKeyVersion?: string,
    bytesRange?: { fromByte: number; toByte: number }
  ) {
    const uploadPartCopy = new UploadPartCopyCommand({
      Bucket: storageS3Bucket,
      Key: withOptionalVersion(key, version),
      UploadId,
      PartNumber,
      CopySource: `${storageS3Bucket}/${withOptionalVersion(sourceKey, sourceKeyVersion)}`,
      CopySourceRange: bytesRange ? `bytes=${bytesRange.fromByte}-${bytesRange.toByte}` : undefined,
    })

    const part = await this.client.send(uploadPartCopy)

    return {
      eTag: part.CopyPartResult?.ETag,
      lastModified: part.CopyPartResult?.LastModified,
    }
  }

  async backup(backupInfo: BackupObjectInfo) {
    return new ObjectBackup(this.client, backupInfo).backup()
  }

  close() {
    this.agent.close()
  }

  protected createS3Client(options: S3ClientOptions & { name: string }) {
    const params: S3ClientConfig = {
      region: options.region,
      runtime: 'node',
      requestHandler: new NodeHttpHandler({
        httpAgent: options.httpAgent?.httpAgent,
        httpsAgent: options.httpAgent?.httpsAgent,
        connectionTimeout: 5000,
        requestTimeout: options.requestTimeout,
      }),
    }
    if (options.endpoint) {
      params.endpoint = options.endpoint
    }
    if (options.forcePathStyle) {
      params.forcePathStyle = true
    }
    return new S3Client(params)
  }
}
