import { Readable } from 'stream'
import { getConfig } from '../../config'

/**
 * Browser cache headers
 */
export interface BrowserCacheHeaders {
  ifModifiedSince?: string
  ifNoneMatch?: string
  range?: string
}

/**
 * Representation of a file object Response
 */
export type ObjectResponse = {
  metadata: ObjectMetadata
  httpStatusCode: number
  body?: ReadableStream<any> | Readable | Blob | Buffer
}

/**
 * Representation of the object metadata
 */
export type ObjectMetadata = {
  cacheControl: string
  contentLength: number
  size: number
  mimetype: string
  lastModified?: Date
  eTag: string
  contentRange?: string
  httpStatusCode?: number
}

export type UploadPart = {
  Version?: string
  ETag?: string
  PartNumber?: number
  ChecksumCRC32?: string
  ChecksumCRC32C?: string
  ChecksumSHA1?: string
  ChecksumSHA256?: string
}

/**
 * A generic storage Adapter to interact with files
 */
export abstract class StorageBackendAdapter {
  client: any
  constructor() {
    this.client = null
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param headers
   */
  async getObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders,
    signal?: AbortSignal
  ): Promise<ObjectResponse> {
    throw new Error('getObject not implemented')
  }

  /**
   * Uploads and store an object
   * @param bucketName
   * @param key
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
    cacheControl: string,
    signal?: AbortSignal
  ): Promise<ObjectMetadata> {
    throw new Error('uploadObject not implemented')
  }

  /**
   * Deletes an object
   * @param bucket
   * @param key
   * @param version
   */
  async deleteObject(bucket: string, key: string, version: string | undefined): Promise<void> {
    throw new Error('deleteObject not implemented')
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
    throw new Error('copyObject not implemented')
  }

  /**
   * Deletes multiple objects
   * @param bucket
   * @param prefixes
   */
  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    throw new Error('deleteObjects not implemented')
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
    throw new Error('headObject not implemented')
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   * @param version
   */
  async privateAssetUrl(bucket: string, key: string, version: string | undefined): Promise<string> {
    throw new Error('privateAssetUrl not implemented')
  }

  async createMultiPartUpload(
    bucketName: string,
    key: string,
    version: string | undefined,
    contentType: string,
    cacheControl: string
  ): Promise<string | undefined> {
    throw new Error('not implemented')
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
  ): Promise<{ ETag?: string }> {
    throw new Error('not implemented')
  }

  async completeMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    version: string,
    parts: UploadPart[]
  ): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    throw new Error('not implemented')
  }

  async abortMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    version?: string
  ): Promise<void> {
    throw new Error('not implemented')
  }

  async uploadPartCopy(
    storageS3Bucket: string,
    key: string,
    version: string,
    UploadId: string,
    PartNumber: number,
    sourceKey: string,
    sourceKeyVersion?: string,
    bytes?: { fromByte: number; toByte: number }
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    throw new Error('not implemented')
  }

  close(): void {
    // do nothing
  }
}

const { tusUseFileVersionSeparator } = getConfig()

export const PATH_SEPARATOR = '/'
export const FILE_VERSION_SEPARATOR = '-$v-'
export const SEPARATOR = tusUseFileVersionSeparator ? FILE_VERSION_SEPARATOR : PATH_SEPARATOR

export function withOptionalVersion(key: string, version?: string): string {
  return version ? `${key}${SEPARATOR}${version}` : key
}
