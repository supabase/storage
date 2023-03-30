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
  httpStatusCode: number
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
    headers?: BrowserCacheHeaders
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
    cacheControl: string
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
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string | undefined
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
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
}

const { tusUseFileVersionSeparator } = getConfig()

export const PATH_SEPARATOR = '/'
export const FILE_VERSION_SEPARATOR = '-$v-'
export const SEPARATOR = tusUseFileVersionSeparator ? FILE_VERSION_SEPARATOR : PATH_SEPARATOR

export function withOptionalVersion(key: string, version?: string): string {
  return version ? `${key}${SEPARATOR}${version}` : key
}
