import stream, { Readable } from 'node:stream'
import { getConfig } from '../../config'

interface BaseParams {
  signal?: AbortSignal
}

export interface ReadParams extends BaseParams {
  bucketName: string
  key: string
  version?: string
  headers?: BrowserCacheHeaders
}

export interface UploadObjectParams extends BaseParams {
  bucketName: string
  key: string
  version?: string
  body: stream.Readable
  contentType: string
  cacheControl: string
}

export interface DeleteObjectParams extends BaseParams {
  bucket: string
  key: string
  version?: string
}

export interface CopyObjectParams extends BaseParams {
  source: {
    bucket: string
    key: string
    version?: string
  }
  destination: {
    bucket: string
    key: string
    version?: string
  }
  metadata?: {
    cacheControl?: string
    mimetype?: string
  }
  conditions?: {
    ifMatch?: string
    ifNoneMatch?: string
    ifModifiedSince?: Date
    ifUnmodifiedSince?: Date
  }
}

export interface DeleteObjectsParams extends BaseParams {
  bucket: string
  keys: string[]
}

export interface HeadObjectParams extends BaseParams {
  bucket: string
  key: string
  version?: string
}

export interface PrivateAssetUrlParams extends BaseParams {
  bucket: string
  key: string
  version?: string
}

export interface CreateMultiPartUploadParams extends BaseParams {
  bucketName: string
  key: string
  version?: string
  contentType: string
  cacheControl: string
}

export interface UploadPartParams extends BaseParams {
  bucketName: string
  key: string
  version: string
  uploadId: string
  partNumber: number
  body?: string | Uint8Array | Buffer | Readable
  length?: number
}

export interface CompleteMultipartUploadParams extends BaseParams {
  bucketName: string
  key: string
  uploadId: string
  version: string
  parts: UploadPart[]
}

export interface AbortMultipartUploadParams extends BaseParams {
  bucketName: string
  key: string
  uploadId: string
  version?: string
}

export interface UploadPartCopyParams extends BaseParams {
  UploadId: string
  PartNumber: number
  source: {
    bucket: string
    key: string
    version?: string
  }
  destination: {
    bucket: string
    key: string
    version?: string
  }
  bytes?: { fromByte: number; toByte: number }
}

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

export interface DiskAdapterOptions {
  mountPoint: string
  prefix?: string
}

/**
 * A generic storage Adapter to interact with files
 */
export abstract class StorageDisk {
  constructor() {}

  withPrefix(prefix: string): StorageDisk {
    throw new Error('not implemented')
  }

  async read(params: ReadParams): Promise<ObjectResponse> {
    throw new Error('getObject not implemented')
  }

  async save(params: UploadObjectParams): Promise<ObjectMetadata> {
    throw new Error('uploadObject not implemented')
  }

  async delete(params: DeleteObjectParams): Promise<void> {
    throw new Error('deleteObject not implemented')
  }

  async copy(
    params: CopyObjectParams
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    throw new Error('copyObject not implemented')
  }

  async deleteMany(params: DeleteObjectsParams): Promise<void> {
    throw new Error('deleteObjects not implemented')
  }

  async info(params: HeadObjectParams): Promise<ObjectMetadata> {
    throw new Error('headObject not implemented')
  }

  async privateAssetUrl(params: PrivateAssetUrlParams): Promise<string> {
    throw new Error('privateAssetUrl not implemented')
  }

  async createMultiPartUpload(params: CreateMultiPartUploadParams): Promise<string | undefined> {
    throw new Error('not implemented')
  }

  async uploadPart(params: UploadPartParams): Promise<{ ETag?: string }> {
    throw new Error('not implemented')
  }

  async completeMultipartUpload(params: CompleteMultipartUploadParams): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    throw new Error('not implemented')
  }

  async abortMultipartUpload(params: AbortMultipartUploadParams): Promise<void> {
    throw new Error('not implemented')
  }

  async uploadPartCopy(
    params: UploadPartCopyParams
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
