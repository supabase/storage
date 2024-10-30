import stream, { Readable } from 'stream'
import { getConfig } from '../../config'

/**
 * Representation of a file object Response
 */
export interface ObjectResponse {
  metadata: ObjectMetadata
  body?: ReadableStream<any> | Readable | Blob | Buffer
  httpStatusCode: number
}

/**
 * Representation of the object metadata
 */
export interface ObjectMetadata {
  cacheControl: string
  contentLength: number
  size: number
  mimetype: string
  lastModified?: Date
  eTag: string
  contentRange?: string

  [key: string]: any
}

export interface UploadPart {
  Version?: string
  ETag?: string
  PartNumber?: number
  ChecksumCRC32?: string
  ChecksumCRC32C?: string
  ChecksumSHA1?: string
  ChecksumSHA256?: string
}

export interface CopyConditions {
  ifMatch?: string
  ifNoneMatch?: string
  ifModifiedSince?: Date
  ifUnmodifiedSince?: Date
}

export interface UploadPartCopyBytes {
  fromByte: number
  toByte: number
}

export interface ReadParams {
  bucket: string
  key: string
  version?: string
  headers?: {
    ifModifiedSince?: string
    ifNoneMatch?: string
    range?: string
  }
  signal?: AbortSignal
}

export interface SaveParams {
  bucket: string
  key: string
  version?: string
  body: stream.Readable
  contentType: string
  cacheControl: string
  signal?: AbortSignal
}

export interface DeleteParams {
  bucket: string
  key: string
  version?: string
  signal?: AbortSignal
}

export interface CopyParams {
  from: {
    bucket: string
    key: string
    version?: string
  }
  to: {
    bucket: string
    key: string
    version?: string
  }
  conditions?: CopyConditions
  signal?: AbortSignal
}

export interface DeleteManyParams {
  bucket: string
  keys: string[]
  signal?: AbortSignal
}

export interface MetadataParams {
  bucket: string
  key: string
  version?: string
  signal?: AbortSignal
}

export interface SignUrlParams {
  bucket: string
  key: string
  version?: string
}

export interface CreateMultiPartUploadParams {
  bucket: string
  key: string
  version?: string
  contentType: string
  cacheControl: string
  signal?: AbortSignal
}

export interface UploadPartParams {
  bucket: string
  key: string
  version: string
  uploadId: string
  partNumber: number
  body?: string | Uint8Array | Buffer | Readable
  length?: number
  signal?: AbortSignal
}

export interface CompleteMultipartUploadParams {
  bucket: string
  key: string
  uploadId: string
  version: string
  parts: UploadPart[]
  signal?: AbortSignal
}

export interface AbortMultipartUploadParams {
  bucket: string
  key: string
  uploadId: string
  version?: string
  signal?: AbortSignal
}

export interface UploadPartCopyParams {
  uploadId: string
  partNumber: number
  from: {
    bucket: string
    key: string
    version?: string
  }
  to: {
    bucket: string
    key: string
    version?: string
  }
  bytes?: UploadPartCopyBytes
  signal?: AbortSignal
}

export interface DiskOptions {
  mountPath: string
  prefix?: string
}

/**
 * A generic storage Adapter to interact with files
 */
export abstract class StorageDisk {
  async read(params: ReadParams): Promise<ObjectResponse> {
    throw new Error('read not implemented')
  }

  async save(params: SaveParams): Promise<ObjectMetadata> {
    throw new Error('save not implemented')
  }

  async delete(params: DeleteParams): Promise<void> {
    throw new Error('delete not implemented')
  }

  async copy(
    params: CopyParams
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    throw new Error('copy not implemented')
  }

  async deleteMany(params: DeleteManyParams): Promise<void> {
    throw new Error('deleteMany not implemented')
  }

  async metadata(params: MetadataParams): Promise<ObjectMetadata> {
    throw new Error('metadata not implemented')
  }

  async signUrl(params: SignUrlParams): Promise<string> {
    throw new Error('signUrl not implemented')
  }

  async createMultiPartUpload(params: CreateMultiPartUploadParams): Promise<string | undefined> {
    throw new Error('createMultiPartUpload not implemented')
  }

  async uploadPart(params: UploadPartParams): Promise<{ ETag?: string }> {
    throw new Error('uploadPart not implemented')
  }

  async completeMultipartUpload(params: CompleteMultipartUploadParams): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    throw new Error('completeMultipartUpload not implemented')
  }

  async abortMultipartUpload(params: AbortMultipartUploadParams): Promise<void> {
    throw new Error('abortMultipartUpload not implemented')
  }

  async uploadPartCopy(
    params: UploadPartCopyParams
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    throw new Error('uploadPartCopy not implemented')
  }

  async close() {}
}

const { tusUseFileVersionSeparator } = getConfig()

export const PATH_SEPARATOR = '/'
export const FILE_VERSION_SEPARATOR = '-$v-'
export const SEPARATOR = tusUseFileVersionSeparator ? FILE_VERSION_SEPARATOR : PATH_SEPARATOR

export function withOptionalVersion(key: string, version?: string): string {
  return version ? `${key}${SEPARATOR}${version}` : key
}
