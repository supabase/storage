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
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
} from './adapter'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { getConfig } from '../../config'
import Agent, { HttpsAgent } from 'agentkeepalive'
import { addAbortSignal, PassThrough, Readable } from 'node:stream'
import {
  HttpPoolErrorGauge,
  HttpPoolFreeSocketsGauge,
  HttpPoolPendingRequestsGauge,
  HttpPoolSocketsGauge,
} from '@internal/monitoring/metrics'
import stream from 'stream/promises'
import { trace } from '@opentelemetry/api'
import { createByteCounterStream } from '@internal/concurrency'

const { storageS3MaxSockets, region } = getConfig()

const watchers: NodeJS.Timeout[] = []

process.once('SIGTERM', () => {
  watchers.forEach((watcher) => {
    clearInterval(watcher)
  })
})

/**
 * Creates an agent for the given protocol
 * @param name
 */
export function createAgent(name: string) {
  const agentOptions = {
    maxSockets: storageS3MaxSockets,
    keepAlive: true,
    keepAliveMsecs: 1000,
    freeSocketTimeout: 1000 * 15,
  }

  const httpAgent = new Agent(agentOptions)
  const httpsAgent = new HttpsAgent(agentOptions)

  if (httpsAgent) {
    const watcher = setInterval(() => {
      const httpStatus = httpAgent.getCurrentStatus()
      const httpsStatus = httpsAgent.getCurrentStatus()
      updateHttpPoolMetrics(name, 'http', httpStatus)
      updateHttpPoolMetrics(name, 'https', httpsStatus)
    }, 5000)

    watchers.push(watcher)
  }

  return { httpAgent, httpsAgent }
}

// Function to update Prometheus metrics based on the current status of the agent
function updateHttpPoolMetrics(name: string, protocol: string, status: Agent.AgentStatus): void {
  // Calculate the number of busy sockets by iterating over the `sockets` object
  let busySocketCount = 0
  for (const host in status.sockets) {
    if (status.sockets.hasOwnProperty(host)) {
      busySocketCount += status.sockets[host]
    }
  }

  // Calculate the number of free sockets by iterating over the `freeSockets` object
  let freeSocketCount = 0
  for (const host in status.freeSockets) {
    if (status.freeSockets.hasOwnProperty(host)) {
      freeSocketCount += status.freeSockets[host]
    }
  }

  // Calculate the number of pending requests by iterating over the `requests` object
  let pendingRequestCount = 0
  for (const host in status.requests) {
    if (status.requests.hasOwnProperty(host)) {
      pendingRequestCount += status.requests[host]
    }
  }

  // Update the metrics with calculated values
  HttpPoolSocketsGauge.set({ name, region, protocol }, busySocketCount)
  HttpPoolFreeSocketsGauge.set({ name, region, protocol }, freeSocketCount)
  HttpPoolPendingRequestsGauge.set({ name, region }, pendingRequestCount)
  HttpPoolErrorGauge.set({ name, region, type: 'socket_error', protocol }, status.errorSocketCount)
  HttpPoolErrorGauge.set(
    { name, region, type: 'timeout_socket_error', protocol },
    status.timeoutSocketCount
  )
  HttpPoolErrorGauge.set(
    { name, region, type: 'create_socket_error', protocol },
    status.createSocketErrorCount
  )
}

export interface S3ClientOptions {
  endpoint?: string
  region?: string
  forcePathStyle?: boolean
  accessKey?: string
  secretKey?: string
  role?: string
  httpAgent?: { httpAgent: Agent; httpsAgent: HttpsAgent }
  requestTimeout?: number
}

/**
 * S3Backend
 * Interacts with a s3-compatible file system with this S3Adapter
 */
export class S3Backend implements StorageBackendAdapter {
  client: S3Client

  constructor(options: S3ClientOptions) {
    // Default client for API operations
    this.client = this.createS3Client({
      ...options,
      name: 's3_default',
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

    const passThrough = new PassThrough()

    if (signal) {
      addAbortSignal(signal, passThrough)
    }

    passThrough.on('error', () => {
      body.unpipe(passThrough)
    })

    body.on('error', (err) => {
      if (!passThrough.closed) {
        passThrough.destroy(err)
      }
    })

    const currentAverage: number[] = []
    const byteReader = createByteCounterStream({
      maxHistory: 100,
      rewriteHistoryOnMax: true,
      onMaxHistory: (history) => {
        currentAverage.push(averageTimeBetweenDates(history))
      },
    })
    const bodyStream = body.pipe(passThrough)

    let upload: Upload | undefined = undefined
    let bytesUploaded = 0

    try {
      const data = await stream.pipeline(
        bodyStream,
        byteReader.transformStream,
        async (bodyStream) => {
          if (signal?.aborted) {
            throw ERRORS.Aborted('Upload was aborted')
          }

          upload = new Upload({
            client: this.client,
            params: {
              Bucket: bucketName,
              Key: withOptionalVersion(key, version),
              Body: bodyStream as Readable,
              ContentType: contentType,
              CacheControl: cacheControl,
            },
          })

          upload.on('httpUploadProgress', (progress) => {
            if (typeof progress.loaded !== 'undefined') {
              bytesUploaded = progress.loaded
            }
          })

          signal?.addEventListener(
            'abort',
            () => {
              upload?.abort()
            },
            { once: true }
          )

          return await upload.done()
        },
        { signal }
      )

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
      if (err instanceof Error && err.name === 'AbortError') {
        const span = trace.getActiveSpan()
        if (span) {
          // Print how far we got uploading the file
          span.setAttributes({
            byteRead: byteReader.bytes,
            bytesUploaded,
            chunkTimes: JSON.stringify([
              ...currentAverage,
              averageTimeBetweenDates(byteReader.history),
            ]),
          })
        }

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
   * @param conditions
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined,
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
        CopySource: `${bucket}/${withOptionalVersion(source, version)}`,
        Key: withOptionalVersion(destination, destinationVersion),
        CopySourceIfMatch: conditions?.ifMatch,
        CopySourceIfNoneMatch: conditions?.ifNoneMatch,
        CopySourceIfModifiedSince: conditions?.ifModifiedSince,
        CopySourceIfUnmodifiedSince: conditions?.ifUnmodifiedSince,
      })
      const data = await this.client.send(command)
      return {
        httpStatusCode: data.$metadata.httpStatusCode || 200,
        eTag: data.CopyObjectResult?.ETag || '',
        lastModified: data.CopyObjectResult?.LastModified,
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
        Key: `${key}/${version}`,
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
    parts: UploadPart[]
  ) {
    const keyParts = key.split('/')

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

  protected createS3Client(options: S3ClientOptions & { name: string }) {
    const agent = options.httpAgent ?? createAgent(options.name)

    const params: S3ClientConfig = {
      region: options.region,
      runtime: 'node',
      requestHandler: new NodeHttpHandler({
        ...agent,
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

function averageTimeBetweenDates(dates: Date[]): number {
  if (dates.length < 2) {
    throw new Error('At least two dates are required to calculate the average time between them.')
  }

  let totalDifference = 0

  for (let i = 1; i < dates.length; i++) {
    const diff = dates[i].getTime() - dates[i - 1].getTime()
    totalDifference += diff
  }

  const averageDifference = totalDifference / (dates.length - 1)
  return averageDifference
}
