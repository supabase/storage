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
  xRobotsTag?: string
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

export interface ListObjectsInput {
  bucket: string
  options?: {
    prefix?: string
    delimiter?: string
    nextToken?: string
    startAfter?: string
    beforeDate?: Date
  }
  signal?: AbortSignal
}

export interface ReadObjectInput {
  bucket: string
  key: string
  version: string | undefined
  headers?: BrowserCacheHeaders
  signal?: AbortSignal
}

export interface WriteObjectInput {
  bucket: string
  key: string
  version: string | undefined
  body: NodeJS.ReadableStream
  contentType: string
  cacheControl: string
  signal?: AbortSignal
}

export interface RemoveObjectInput {
  bucket: string
  key: string
  version: string | undefined
  signal?: AbortSignal
}

export interface CopyObjectInput {
  bucket: string
  source: string
  version: string | undefined
  destination: string
  destinationVersion: string | undefined
  metadata?: { cacheControl?: string; mimetype?: string }
  conditions?: {
    ifMatch?: string
    ifNoneMatch?: string
    ifModifiedSince?: Date
    ifUnmodifiedSince?: Date
  }
  signal?: AbortSignal
}

export interface RemoveManyObjectsInput {
  bucket: string
  prefixes: string[]
  signal?: AbortSignal
}

export interface StatsObjectInput {
  bucket: string
  key: string
  version: string | undefined
  signal?: AbortSignal
}

export interface TempPrivateAccessUrlInput {
  bucket: string
  key: string
  version: string | undefined
  signal?: AbortSignal
}

export interface CreateMultiPartUploadInput {
  bucket: string
  key: string
  version: string | undefined
  contentType: string
  cacheControl: string
  metadata?: Record<string, string>
  signal?: AbortSignal
}

export interface UploadPartInput {
  bucket: string
  key: string
  version: string
  uploadId: string
  partNumber: number
  body?: string | Uint8Array | Buffer | Readable
  length?: number
  signal?: AbortSignal
}

export interface CompleteMultipartUploadInput {
  bucket: string
  key: string
  uploadId: string
  version: string
  parts: UploadPart[]
  opts?: { removePrefix?: boolean }
  signal?: AbortSignal
}

export interface AbortMultipartUploadInput {
  bucket: string
  key: string
  uploadId: string
  version?: string
  signal?: AbortSignal
}

export interface UploadPartCopyInput {
  bucket: string
  key: string
  version: string
  uploadId: string
  partNumber: number
  sourceKey: string
  sourceKeyVersion?: string
  bytesRange?: { fromByte: number; toByte: number }
  signal?: AbortSignal
}

/**
 * A generic storage Adapter to interact with files
 */
export abstract class StorageBackendAdapter {
  client: any
  constructor() {
    this.client = null
  }

  async list(
    input: ListObjectsInput
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    throw new Error('list not implemented')
  }

  /**
   * Gets an object body and metadata
   */
  async read(input: ReadObjectInput): Promise<ObjectResponse> {
    throw new Error('getObject not implemented')
  }

  /**
   * Uploads and store an object
   */
  async write(input: WriteObjectInput): Promise<ObjectMetadata> {
    throw new Error('uploadObject not implemented')
  }

  /**
   * Deletes an object
   */
  async remove(input: RemoveObjectInput): Promise<void> {
    throw new Error('deleteObject not implemented')
  }

  /**
   * Copies an existing object to the given location
   */
  async copy(
    input: CopyObjectInput
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    throw new Error('copyObject not implemented')
  }

  /**
   * Deletes multiple objects
   */
  async removeMany(input: RemoveManyObjectsInput): Promise<void> {
    throw new Error('deleteObjects not implemented')
  }

  /**
   * Returns metadata information of a specific object
   */
  async stats(input: StatsObjectInput): Promise<ObjectMetadata> {
    throw new Error('headObject not implemented')
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   */
  async tempPrivateAccessUrl(input: TempPrivateAccessUrlInput): Promise<string> {
    throw new Error('privateAssetUrl not implemented')
  }

  async createMultiPartUpload(input: CreateMultiPartUploadInput): Promise<string | undefined> {
    throw new Error('not implemented')
  }

  async uploadPart(input: UploadPartInput): Promise<{ ETag?: string }> {
    throw new Error('not implemented')
  }

  async completeMultipartUpload(input: CompleteMultipartUploadInput): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    throw new Error('not implemented')
  }

  async abortMultipartUpload(input: AbortMultipartUploadInput): Promise<void> {
    throw new Error('not implemented')
  }

  async uploadPartCopy(
    input: UploadPartCopyInput
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
