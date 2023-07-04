import {
  CompleteMultipartUploadCommandOutput,
  CopyObjectCommand,
  CreateBucketCommand,
  CreateBucketCommandInput,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  HeadBucketCommand,
  HeadBucketCommandInput,
  HeadObjectCommand,
  S3Client,
  S3ClientConfig,
  S3ServiceException,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'
import {
  StorageBackendAdapter,
  BrowserCacheHeaders,
  ObjectMetadata,
  ObjectResponse,
  withPrefixAndVersion,
} from './generic'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { StorageBackendError } from '../errors'
import { getConfig } from '../../config'
import Agent, { HttpsAgent } from 'agentkeepalive'

const { globalS3Protocol, globalS3MaxSockets, region, globalS3Endpoint, globalS3ForcePathStyle } =
  getConfig()

export interface S3ClientOptions {
  endpoint?: string
  region?: string
  forcePathStyle?: boolean
  accessKey?: string
  secretKey?: string
  role?: string
}

export interface S3Options {
  bucket: string
  prefix?: string
  client: S3Client | S3ClientOptions
}

const defaultAgent = createAgent()
let defaultS3Client: S3Client | undefined = undefined

/**
 * Get default S3 Client
 * @param options
 */
export function getS3DefaultClient() {
  if (defaultS3Client) {
    return defaultS3Client
  }

  defaultS3Client = createS3Client({
    endpoint: globalS3Endpoint,
    region,
    forcePathStyle: globalS3ForcePathStyle,
  })
  return defaultS3Client
}

function createAgent() {
  const agentOptions = {
    maxSockets: globalS3MaxSockets,
    keepAlive: true,
  }

  return globalS3Protocol === 'http'
    ? { httpAgent: new Agent(agentOptions) }
    : { httpsAgent: new HttpsAgent(agentOptions) }
}

export function createS3Client(options: S3ClientOptions): S3Client {
  const params: S3ClientConfig = {
    region: options.region,
    runtime: 'node',
    requestHandler: new NodeHttpHandler({
      ...defaultAgent,
    }),
  }
  if (options.endpoint) {
    params.endpoint = options.endpoint
  }
  if (options.forcePathStyle) {
    params.forcePathStyle = true
  }
  if (options.accessKey && options.secretKey) {
    params.credentials = {
      accessKeyId: options.accessKey,
      secretAccessKey: options.secretKey,
    }
  }

  if (options.role) {
    // TODO: assume role
  }

  return new S3Client(params)
}

/**
 * S3Backend
 * Interacts with an s3-compatible file system with this S3Adapter
 */
export class S3Backend implements StorageBackendAdapter {
  client: S3Client

  constructor(private readonly options: S3Options) {
    this.client =
      options.client instanceof S3Client ? options.client : createS3Client(options.client)
  }

  async createBucketIfDoesntExists(bucketName: string) {
    const bucketExists = await this.checkBucketExists(bucketName)

    if (bucketExists) {
      return
    }
    const input: CreateBucketCommandInput = {
      Bucket: bucketName,
    }

    return this.client.send(new CreateBucketCommand(input))
  }

  async checkBucketExists(bucketName: string) {
    const input: HeadBucketCommandInput = {
      Bucket: bucketName,
    }

    try {
      await this.client.send(new HeadBucketCommand(input))

      return true
    } catch (e: unknown) {
      if (e instanceof S3ServiceException && e.$metadata.httpStatusCode === 404) {
        return false
      }
      throw e
    }
  }

  /**
   * Gets an object body and metadata
   * @param key
   * @param version
   * @param headers
   */
  async getObject(
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders
  ): Promise<ObjectResponse> {
    const input: GetObjectCommandInput = {
      Bucket: this.options.bucket,
      IfNoneMatch: headers?.ifNoneMatch,
      Key: withPrefixAndVersion(key, this.options.prefix, version),
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
   * @param key
   * @param version
   * @param body
   * @param contentType
   * @param cacheControl
   */
  async uploadObject(
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
          Bucket: this.options.bucket,
          Key: withPrefixAndVersion(key, this.options.prefix, version),
          /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
          Body: body,
          ContentType: contentType,
          CacheControl: cacheControl,
        },
      })

      const data = (await paralellUploadS3.done()) as CompleteMultipartUploadCommandOutput

      const metadata = await this.headObject(key, version)

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

  async setMetadataToCompleted(key: string) {
    const headObject = new HeadObjectCommand({
      Bucket: this.options.bucket,
      Key: `${key}.info`,
    })
    const findObjResp = await this.client.send(headObject)

    const copyCmd = new CopyObjectCommand({
      Bucket: this.options.bucket,
      CopySource: `${this.options.bucket}/${withPrefixAndVersion(key, this.options.prefix)}.info`,
      Key: `${withPrefixAndVersion(key, this.options.prefix)}.info`,
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
   * @param key
   * @param version
   */
  async deleteObject(key: string, version: string | undefined): Promise<void> {
    const command = new DeleteObjectCommand({
      Bucket: this.options.bucket,
      Key: withPrefixAndVersion(key, this.options.prefix, version),
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
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
    try {
      const command = new CopyObjectCommand({
        Bucket: this.options.bucket,
        CopySource: `${this.options.bucket}/${withPrefixAndVersion(
          source,
          this.options.prefix,
          version
        )}`,
        Key: withPrefixAndVersion(destination, this.options.prefix, destinationVersion),
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
  async deleteObjects(prefixes: string[]): Promise<void> {
    try {
      const s3Prefixes = prefixes.map((ele) => {
        return { Key: withPrefixAndVersion(ele, this.options.prefix) }
      })

      const command = new DeleteObjectsCommand({
        Bucket: this.options.bucket,
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
  async headObject(key: string, version: string | undefined): Promise<ObjectMetadata> {
    try {
      const command = new HeadObjectCommand({
        Bucket: this.options.bucket,
        Key: withPrefixAndVersion(key, this.options.prefix, version),
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
  async privateAssetUrl(key: string, version: string | undefined): Promise<string> {
    const input: GetObjectCommandInput = {
      Bucket: this.options.bucket,
      Key: withPrefixAndVersion(key, this.options.prefix, version),
    }

    const command = new GetObjectCommand(input)
    return getSignedUrl(this.client, command, { expiresIn: 600 })
  }
}
