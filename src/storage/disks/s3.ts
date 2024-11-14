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
  ListPartsCommand,
  S3Client,
  S3ClientConfig,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { Progress, Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import {
  StorageDisk,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  ReadParams,
  UploadObjectParams,
  DeleteObjectParams,
  CopyObjectParams,
  DeleteObjectsParams,
  HeadObjectParams,
  PrivateAssetUrlParams,
  CreateMultiPartUploadParams,
  UploadPartParams,
  CompleteMultipartUploadParams,
  AbortMultipartUploadParams,
  UploadPartCopyParams,
  DiskAdapterOptions,
} from './adapter'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../config'
import { createAgent } from '@internal/http'
import { monitorStream } from '@internal/streams'

const { tracingFeatures, storageS3MaxSockets, tracingEnabled } = getConfig()

export interface S3DiskOptions extends DiskAdapterOptions {
  client: S3Client
  requestTimeout?: number
}

export interface S3Credentials {
  name: string
  region: string
  endpoint?: string
  forcePathStyle?: boolean
  requestTimeout?: number
}

export function createS3Client(options: S3Credentials): S3Client {
  const agent = createAgent(options.name, {
    maxSockets: storageS3MaxSockets,
  })

  const params: S3ClientConfig = {
    region: options.region,
    runtime: 'node',
    requestHandler: new NodeHttpHandler({
      httpAgent: agent?.httpAgent,
      httpsAgent: agent?.httpsAgent,
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

  if (agent && tracingEnabled) {
    agent.monitor()
  }

  return new S3Client(params)
}

/**
 * S3Backend
 * Interacts with a s3-compatible file system with this S3Adapter
 */
export class S3Disk implements StorageDisk {
  client: S3Client
  mountBucket: string
  prefix?: string

  constructor(protected readonly options: S3DiskOptions) {
    this.client = options.client
    this.mountBucket = options.mountPoint
    this.prefix = options.prefix
  }

  withPrefix(prefix: string) {
    return new S3Disk({
      ...this.options,
      prefix,
    })
  }

  /**
   * Gets an object body and metadata
   * @param params
   */
  async read(params: ReadParams): Promise<ObjectResponse> {
    const { bucketName, key, version, headers, signal } = params

    const input: GetObjectCommandInput = {
      Bucket: this.mountBucket,
      IfNoneMatch: headers?.ifNoneMatch,
      Key: this.makeObjectKey(bucketName, key, version),
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
   * @param params
   */
  async save(params: UploadObjectParams): Promise<ObjectMetadata> {
    const { bucketName, key, version, body, contentType, cacheControl, signal } = params

    if (signal?.aborted) {
      throw ERRORS.Aborted('Upload was aborted')
    }

    const dataStream = tracingFeatures?.upload ? monitorStream(body) : body

    const upload = new Upload({
      client: this.client,
      params: {
        Bucket: this.mountBucket,
        Key: this.makeObjectKey(bucketName, key, version),
        Body: dataStream,
        ContentType: contentType,
        CacheControl: cacheControl,
      },
    })

    signal?.addEventListener(
      'abort',
      () => {
        upload.abort()
      },
      { once: true }
    )

    if (tracingFeatures?.upload) {
      upload.on('httpUploadProgress', (progress: Progress) => {
        dataStream.emit('s3_progress', JSON.stringify(progress))
      })
    }

    try {
      const data = await upload.done()
      const metadata = await this.info({ bucket: bucketName, key, version })

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
      if (err instanceof Error && err.name === 'AbortError') {
        throw ERRORS.AbortedTerminate('Upload was aborted', err)
      }
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Deletes an object
   * @param params
   */
  async delete(params: DeleteObjectParams): Promise<void> {
    const { bucket, key, version } = params

    const command = new DeleteObjectCommand({
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(bucket, key, version),
    })
    await this.client.send(command)
  }

  /**
   * Copies an existing object to the given location
   * @param params
   */
  async copy(
    params: CopyObjectParams
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const { source, destination, metadata, conditions } = params

    try {
      const command = new CopyObjectCommand({
        Bucket: this.mountBucket,
        Key: this.makeObjectKey(destination.bucket, destination.key, destination.version),
        CopySource: `${this.mountBucket}/${this.makeObjectKey(
          source.bucket,
          source.key,
          source.version
        )}`,
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
    } catch (e: unknown) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Deletes multiple objects
   * @param params
   */
  async deleteMany(params: DeleteObjectsParams): Promise<void> {
    const { keys, bucket } = params
    try {
      const s3Keys = keys.map((ele) => {
        return { Key: `${bucket}/${ele}` }
      })

      const command = new DeleteObjectsCommand({
        Bucket: this.mountBucket,
        Delete: {
          Objects: s3Keys,
        },
      })
      await this.client.send(command)
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns metadata information of a specific object
   * @param params
   */
  async info(params: HeadObjectParams): Promise<ObjectMetadata> {
    const { bucket, key, version } = params
    try {
      const command = new HeadObjectCommand({
        Bucket: this.mountBucket,
        Key: this.makeObjectKey(bucket, key, version),
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
    } catch (e: unknown) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param params
   */
  async privateAssetUrl(params: PrivateAssetUrlParams): Promise<string> {
    const { bucket, key, version } = params
    const input: GetObjectCommandInput = {
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(bucket, key, version),
    }

    const command = new GetObjectCommand(input)
    return getSignedUrl(this.client, command, { expiresIn: 600 })
  }

  async createMultiPartUpload(params: CreateMultiPartUploadParams) {
    const { bucketName, key, version, cacheControl, contentType } = params
    const createMultiPart = new CreateMultipartUploadCommand({
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(bucketName, key, version),
      CacheControl: cacheControl,
      ContentType: contentType,
      Metadata: {
        Version: version || '',
      },
    })

    const resp = await this.client.send(createMultiPart)

    if (!resp.UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    return resp.UploadId
  }

  async uploadPart(params: UploadPartParams) {
    const { bucketName, key, version, uploadId, partNumber, body, length, signal } = params
    try {
      const paralellUploadS3 = new UploadPartCommand({
        Bucket: this.mountBucket,
        Key: this.makeObjectKey(bucketName, key, version),
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

  async completeMultipartUpload({
    bucketName,
    key,
    uploadId,
    version,
    parts,
  }: CompleteMultipartUploadParams) {
    const keyParts = key.split('/')

    if (parts.length === 0) {
      const listPartsInput = new ListPartsCommand({
        Bucket: this.mountBucket,
        Key: this.makeObjectKey(bucketName, key, version),
        UploadId: uploadId,
      })

      const partsResponse = await this.client.send(listPartsInput)
      parts = partsResponse.Parts || []
    }

    const completeUpload = new CompleteMultipartUploadCommand({
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(bucketName, key, version),
      UploadId: uploadId,
      MultipartUpload:
        parts.length === 0
          ? undefined
          : {
              Parts: parts,
            },
    })

    const response = await this.client.send(completeUpload)

    const locationParts = key.split('/')
    locationParts.shift() // tenant-id
    const bucket = keyParts.shift()

    return {
      version,
      location: keyParts.join('/'),
      bucket,
      ...response,
    }
  }

  /**
   * Aborts a multipart upload
   * @param params
   */
  async abortMultipartUpload(params: AbortMultipartUploadParams): Promise<void> {
    const { bucketName, key, uploadId } = params
    const abortUpload = new AbortMultipartUploadCommand({
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(bucketName, key),
      UploadId: uploadId,
    })
    await this.client.send(abortUpload)
  }

  /**
   * Uploads a part from an existing object
   * @param params
   */
  async uploadPartCopy(params: UploadPartCopyParams) {
    const { source, destination, UploadId, PartNumber, bytes } = params

    const uploadPartCopy = new UploadPartCopyCommand({
      Bucket: this.mountBucket,
      Key: this.makeObjectKey(destination.bucket, destination.key, destination.version),
      UploadId,
      PartNumber,
      CopySource: `${this.mountBucket}/${this.makeObjectKey(
        source.bucket,
        source.key,
        source.version
      )}`,
      CopySourceRange: bytes ? `bytes=${bytes.fromByte}-${bytes.toByte}` : undefined,
    })

    const part = await this.client.send(uploadPartCopy)

    return {
      eTag: part.CopyPartResult?.ETag,
      lastModified: part.CopyPartResult?.LastModified,
    }
  }

  /**
   * Closes the agent
   */
  close() {
    this.client.destroy()
  }

  protected makeObjectKey(bucket: string, key: string, version?: string) {
    const keyPath = `${bucket}/${withOptionalVersion(key, version)}`

    if (this.prefix) {
      return `${this.prefix}/${keyPath}`
    }
    return keyPath
  }
}
