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
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
  ListObjectsInput,
  ReadObjectInput,
  WriteObjectInput,
  RemoveObjectInput,
  CopyObjectInput,
  RemoveManyObjectsInput,
  StatsObjectInput,
  TempPrivateAccessUrlInput,
  CreateMultiPartUploadInput,
  UploadPartInput,
  CompleteMultipartUploadInput,
  AbortMultipartUploadInput,
  UploadPartCopyInput,
} from './../adapter'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../../config'
import { Readable } from 'node:stream'
import { createAgent, InstrumentedAgent } from '@internal/http'
import { monitorStream } from '@internal/streams'
import { BackupObjectInfo, ObjectBackup } from '@storage/backend/s3/s3-backup'

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
   */
  async read(input: ReadObjectInput): Promise<ObjectResponse> {
    const { bucket, key, version, headers, signal } = input
    const commandInput: GetObjectCommandInput = {
      Bucket: bucket,
      IfNoneMatch: headers?.ifNoneMatch,
      Key: withOptionalVersion(key, version),
      Range: headers?.range,
    }
    if (headers?.ifModifiedSince) {
      commandInput.IfModifiedSince = new Date(headers.ifModifiedSince)
    }
    const command = new GetObjectCommand(commandInput)
    const data = await this.client.send(command, {
      abortSignal: signal,
    })

    return {
      metadata: {
        cacheControl: data.CacheControl || 'no-cache',
        mimetype: data.ContentType || 'application/octet-stream',
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
   */
  async write(input: WriteObjectInput): Promise<ObjectMetadata> {
    const { bucket, key, version, body, contentType, cacheControl, signal } = input

    if (signal?.aborted) {
      throw ERRORS.Aborted('Upload was aborted')
    }

    const readableBody = body as Readable
    const dataStream = tracingFeatures?.upload ? monitorStream(readableBody) : readableBody

    const upload = new Upload({
      client: this.client,
      queueSize: storageS3UploadQueueSize,
      params: {
        Bucket: bucket,
        Key: withOptionalVersion(key, version),
        Body: dataStream as Readable,
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
        ;(dataStream as any).emit('s3_progress', JSON.stringify(progress))
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
        ? await this.stats({ bucket, key, version })
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
   */
  async remove(input: RemoveObjectInput): Promise<void> {
    const { bucket, key, version } = input
    const command = new DeleteObjectCommand({
      Bucket: bucket,
      Key: withOptionalVersion(key, version),
    })
    await this.client.send(command)
  }

  /**
   * Copies an existing object to the given location
   */
  async copy(
    input: CopyObjectInput
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const { bucket, source, version, destination, destinationVersion, metadata, conditions } = input
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
    input: ListObjectsInput
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    const { bucket, options } = input
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
   */
  async removeMany(input: RemoveManyObjectsInput): Promise<void> {
    const { bucket, prefixes } = input
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
   */
  async stats(input: StatsObjectInput): Promise<ObjectMetadata> {
    const { bucket, key, version } = input
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
   */
  async tempPrivateAccessUrl(input: TempPrivateAccessUrlInput): Promise<string> {
    const { bucket, key, version } = input
    const commandInput: GetObjectCommandInput = {
      Bucket: bucket,
      Key: withOptionalVersion(key, version),
    }

    const command = new GetObjectCommand(commandInput)
    return getSignedUrl(this.client, command, { expiresIn: 600 })
  }

  async createMultiPartUpload(input: CreateMultiPartUploadInput) {
    const { bucket, key, version, contentType, cacheControl, metadata } = input
    const createMultiPart = new CreateMultipartUploadCommand({
      Bucket: bucket,
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

  async uploadPart(input: UploadPartInput) {
    const { bucket, key, version, uploadId, partNumber, body, length, signal } = input
    try {
      const paralellUploadS3 = new UploadPartCommand({
        Bucket: bucket,
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

  async completeMultipartUpload(input: CompleteMultipartUploadInput) {
    const { bucket, key, uploadId, version, opts } = input
    let { parts } = input
    const keyParts = key.split('/')

    if (parts.length === 0) {
      const listPartsInput = new ListPartsCommand({
        Bucket: bucket,
        Key: version ? key + '/' + version : key,
        UploadId: uploadId,
      })

      const partsResponse = await this.client.send(listPartsInput)
      parts = partsResponse.Parts || []
    }

    const completeUpload = new CompleteMultipartUploadCommand({
      Bucket: bucket,
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
    let resultBucket = bucket

    if (opts?.removePrefix) {
      const locationParts = key.split('/')
      locationParts.shift() // tenant-id

      resultBucket = keyParts.shift() || ''
      location = keyParts.join('/')
    }

    return {
      version,
      location: location,
      bucket: resultBucket,
      ...response,
    }
  }

  async abortMultipartUpload(input: AbortMultipartUploadInput): Promise<void> {
    const { bucket, key, uploadId } = input
    const abortUpload = new AbortMultipartUploadCommand({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
    })
    await this.client.send(abortUpload)
  }

  async uploadPartCopy(input: UploadPartCopyInput) {
    const { bucket, key, version, uploadId, partNumber, sourceKey, sourceKeyVersion, bytesRange } =
      input
    const uploadPartCopyCmd = new UploadPartCopyCommand({
      Bucket: bucket,
      Key: withOptionalVersion(key, version),
      UploadId: uploadId,
      PartNumber: partNumber,
      CopySource: `${bucket}/${withOptionalVersion(sourceKey, sourceKeyVersion)}`,
      CopySourceRange: bytesRange ? `bytes=${bytesRange.fromByte}-${bytesRange.toByte}` : undefined,
    })

    const part = await this.client.send(uploadPartCopyCmd)

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
      requestStreamBufferSize: 32 * 1024,
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
