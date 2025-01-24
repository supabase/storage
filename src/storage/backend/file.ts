import * as xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import MultiStream from 'multistream'
import { getConfig } from '../../config'
import {
  StorageBackendAdapter,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  BrowserCacheHeaders,
  UploadPart,
} from './adapter'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { randomUUID } from 'crypto'
import fsExtra from 'fs-extra'
const pipeline = promisify(stream.pipeline)

interface FileMetadata {
  cacheControl: string
  contentType: string
}

// file metadata attribute keys on different platforms
const METADATA_ATTR_KEYS = {
  darwin: {
    'cache-control': 'com.apple.metadata.supabase.cache-control',
    'content-type': 'com.apple.metadata.supabase.content-type',
    etag: 'com.apple.metadata.supabase.etag',
  },
  linux: {
    'cache-control': 'user.supabase.cache-control',
    'content-type': 'user.supabase.content-type',
    etag: 'user.supabase.content-type',
  },
}

/**
 * FileBackend
 * Interacts with the file system with this FileBackend adapter
 */
export class FileBackend implements StorageBackendAdapter {
  client = null
  filePath: string
  etagAlgorithm: 'mtime' | 'md5'

  constructor() {
    const { storageFilePath, storageFileEtagAlgorithm } = getConfig()
    if (!storageFilePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = storageFilePath
    this.etagAlgorithm = storageFileEtagAlgorithm
  }

  private async etag(file: string, stats: fs.Stats): Promise<string> {
    if (this.etagAlgorithm === 'md5') {
      const checksum = await fileChecksum(file)
      return `"${checksum}"`
    } else if (this.etagAlgorithm === 'mtime') {
      return `"${stats.mtimeMs.toString(16)}-${stats.size.toString(16)}"`
    }
    throw new Error('FILE_STORAGE_ETAG_ALGORITHM env variable must be either "mtime" or "md5"')
  }

  async list(
    bucket: string,
    options?: {
      prefix?: string
      delimiter?: string
      nextToken?: string
      startAfter?: string
    }
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    return Promise.resolve({ keys: [] })
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param version
   * @param headers
   */
  async getObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    headers?: BrowserCacheHeaders
  ): Promise<ObjectResponse> {
    // 'Range: bytes=#######-######
    const file = path.resolve(this.filePath, withOptionalVersion(`${bucketName}/${key}`, version))
    const data = await fs.stat(file)
    const eTag = await this.etag(file, data)
    const fileSize = data.size
    const { cacheControl, contentType } = await this.getFileMetadata(file)
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)

    if (headers?.ifNoneMatch && headers.ifNoneMatch === eTag) {
      return {
        metadata: {
          cacheControl: cacheControl || 'no-cache',
          mimetype: contentType || 'application/octet-stream',
          lastModified: lastModified,
          httpStatusCode: 304,
          size: data.size,
          eTag,
          contentLength: 0,
        },
        body: undefined,
        httpStatusCode: 304,
      }
    }

    if (headers?.ifModifiedSince) {
      const ifModifiedSince = new Date(headers.ifModifiedSince)
      if (lastModified <= ifModifiedSince) {
        return {
          metadata: {
            cacheControl: cacheControl || 'no-cache',
            mimetype: contentType || 'application/octet-stream',
            lastModified: lastModified,
            httpStatusCode: 304,
            size: data.size,
            eTag,
            contentLength: 0,
          },
          body: undefined,
          httpStatusCode: 304,
        }
      }
    }

    if (headers?.range) {
      const parts = headers.range.replace(/bytes=/, '').split('-')
      const startRange = parseInt(parts[0], 10)
      const endRange = parts[1] ? parseInt(parts[1], 10) : fileSize - 1
      const size = endRange - startRange
      const chunkSize = size + 1
      const body = fs.createReadStream(file, { start: startRange, end: endRange })

      return {
        metadata: {
          cacheControl: cacheControl || 'no-cache',
          mimetype: contentType || 'application/octet-stream',
          lastModified: lastModified,
          contentRange: `bytes ${startRange}-${endRange}/${fileSize}`,
          httpStatusCode: 206,
          size: size,
          eTag,
          contentLength: chunkSize,
        },
        httpStatusCode: 206,
        body,
      }
    } else {
      const body = fs.createReadStream(file)
      return {
        metadata: {
          cacheControl: cacheControl || 'no-cache',
          mimetype: contentType || 'application/octet-stream',
          lastModified: lastModified,
          httpStatusCode: 200,
          size: data.size,
          eTag,
          contentLength: fileSize,
        },
        body,
        httpStatusCode: 200,
      }
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
   */
  async uploadObject(
    bucketName: string,
    key: string,
    version: string | undefined,
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    try {
      const file = path.resolve(this.filePath, withOptionalVersion(`${bucketName}/${key}`, version))
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, {
        contentType,
        cacheControl,
      })

      const metadata = await this.headObject(bucketName, key, version)

      return {
        ...metadata,
        httpStatusCode: 200,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Deletes an object from the file system
   * @param bucket
   * @param key
   * @param version
   */
  async deleteObject(bucket: string, key: string, version: string | undefined): Promise<void> {
    try {
      const file = path.resolve(this.filePath, withOptionalVersion(`${bucket}/${key}`, version))
      await fs.remove(file)
    } catch (e) {
      if (e instanceof Error && 'code' in e) {
        if ((e as any).code === 'ENOENT') {
          return
        }
        throw e
      }
    }
  }

  /**
   * Copies an existing object to the given location
   * @param bucket
   * @param source
   * @param version
   * @param destination
   * @param destinationVersion
   * @param metadata
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string,
    metadata: { cacheControl?: string; contentType?: string }
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const srcFile = path.resolve(this.filePath, withOptionalVersion(`${bucket}/${source}`, version))
    const destFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${bucket}/${destination}`, destinationVersion)
    )

    await fs.ensureFile(destFile)
    await fs.copyFile(srcFile, destFile)

    const originalMetadata = await this.getFileMetadata(srcFile)
    await this.setFileMetadata(destFile, Object.assign({}, originalMetadata, metadata))

    const fileStat = await fs.lstat(destFile)
    const eTag = await this.etag(destFile, fileStat)

    return {
      httpStatusCode: 200,
      lastModified: fileStat.mtime,
      eTag,
    }
  }

  /**
   * Deletes multiple objects
   * @param bucket
   * @param prefixes
   */
  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    const promises = prefixes.map((prefix) => {
      return fs.rm(path.resolve(this.filePath, bucket, prefix))
    })
    const results = await Promise.allSettled(promises)

    results.forEach((result) => {
      if (result.status === 'rejected') {
        if (result.reason.code === 'ENOENT') {
          return
        }
        throw result.reason
      }
    })
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
    const file = path.join(this.filePath, withOptionalVersion(`${bucket}/${key}`, version))

    const data = await fs.stat(file)
    const { cacheControl, contentType } = await this.getFileMetadata(file)
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)
    const eTag = await this.etag(file, data)

    return {
      httpStatusCode: 200,
      size: data.size,
      cacheControl: cacheControl || 'no-cache',
      mimetype: contentType || 'application/octet-stream',
      eTag,
      lastModified: data.birthtime,
      contentLength: data.size,
    }
  }

  async createMultiPartUpload(
    bucketName: string,
    key: string,
    version: string | undefined,
    contentType: string,
    cacheControl: string
  ): Promise<string | undefined> {
    const uploadId = randomUUID()
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucketName,
      withOptionalVersion(key, version)
    )

    const multipartFile = path.join(multiPartFolder, 'metadata.json')
    await fsExtra.ensureDir(multiPartFolder)
    await fsExtra.writeFile(multipartFile, JSON.stringify({ contentType, cacheControl }))

    return uploadId
  }

  async uploadPart(
    bucketName: string,
    key: string,
    version: string,
    uploadId: string,
    partNumber: number,
    body: stream.Readable
  ): Promise<{ ETag?: string }> {
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucketName,
      withOptionalVersion(key, version)
    )

    const partPath = path.join(multiPartFolder, `part-${partNumber}`)

    const writeStream = fsExtra.createWriteStream(partPath)

    await pipeline(body, writeStream)

    const etag = await fileChecksum(partPath)

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await this.setMetadataAttr(partPath, METADATA_ATTR_KEYS[platform]['etag'], etag)

    return { ETag: etag }
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
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucketName,
      withOptionalVersion(key, version)
    )

    const partsByEtags = parts.map(async (part) => {
      const partFilePath = path.join(multiPartFolder, `part-${part.PartNumber}`)
      const partExists = await fsExtra.pathExists(partFilePath)

      if (partExists) {
        const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
        const etag = await this.getMetadataAttr(partFilePath, METADATA_ATTR_KEYS[platform]['etag'])
        if (etag === part.ETag) {
          return partFilePath
        }
        throw ERRORS.InvalidChecksum(`Invalid ETag for part ${part.PartNumber}`)
      }

      throw ERRORS.MissingPart(part.PartNumber || 0, uploadId)
    })

    const finalParts = await Promise.all(partsByEtags)
    finalParts.sort((a, b) => parseInt(a.split('-')[1]) - parseInt(b.split('-')[1]))

    const fileStreams = finalParts.map((partPath) => {
      return fs.createReadStream(partPath)
    })

    const multistream = new MultiStream(fileStreams)
    const metadataContent = await fsExtra.readFile(
      path.join(multiPartFolder, 'metadata.json'),
      'utf-8'
    )

    const metadata = JSON.parse(metadataContent)

    const uploaded = await this.uploadObject(
      bucketName,
      key,
      version,
      multistream,
      metadata.contentType,
      metadata.cacheControl
    )

    fsExtra.remove(path.join(this.filePath, 'multiparts', uploadId)).catch(() => {
      // no-op
    })

    return {
      version: version,
      ETag: uploaded.eTag,
      bucket: bucketName,
      location: `${bucketName}/${key}`,
    }
  }

  async abortMultipartUpload(
    bucketName: string,
    key: string,
    uploadId: string,
    version?: string
  ): Promise<void> {
    const multiPartFolder = path.join(this.filePath, 'multiparts', uploadId)

    await fsExtra.remove(multiPartFolder)
  }

  async uploadPartCopy(
    storageS3Bucket: string,
    key: string,
    version: string,
    UploadId: string,
    PartNumber: number,
    sourceKey: string,
    sourceVersion?: string,
    rangeBytes?: { fromByte: number; toByte: number }
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      UploadId,
      storageS3Bucket,
      withOptionalVersion(key, version)
    )

    const partFilePath = path.join(multiPartFolder, `part-${PartNumber}`)
    const sourceFilePath = path.join(
      this.filePath,
      storageS3Bucket,
      withOptionalVersion(sourceKey, sourceVersion)
    )

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'

    const readStreamOptions = rangeBytes
      ? { start: rangeBytes.fromByte, end: rangeBytes.toByte }
      : {}
    const partStream = fs.createReadStream(sourceFilePath, readStreamOptions)

    const writePart = fs.createWriteStream(partFilePath)
    await pipeline(partStream, writePart)

    const etag = await fileChecksum(partFilePath)
    await this.setMetadataAttr(partFilePath, METADATA_ATTR_KEYS[platform]['etag'], etag)

    const fileStat = await fs.lstat(partFilePath)

    return {
      eTag: etag,
      lastModified: fileStat.mtime,
    }
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   * @param version
   */
  async privateAssetUrl(bucket: string, key: string, version: string | undefined): Promise<string> {
    return 'local:///' + path.join(this.filePath, withOptionalVersion(`${bucket}/${key}`, version))
  }

  async setFileMetadata(file: string, { contentType, cacheControl }: FileMetadata) {
    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await Promise.all([
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['cache-control'], cacheControl),
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['content-type'], contentType),
    ])
  }

  close() {
    // no-op
  }

  protected async getFileMetadata(file: string) {
    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    const [cacheControl, contentType] = await Promise.all([
      this.getMetadataAttr(file, METADATA_ATTR_KEYS[platform]['cache-control']),
      this.getMetadataAttr(file, METADATA_ATTR_KEYS[platform]['content-type']),
    ])

    return {
      cacheControl,
      contentType,
    } as FileMetadata
  }

  protected getMetadataAttr(file: string, attribute: string): Promise<string | undefined> {
    return xattr.get(file, attribute).then((value: any) => {
      return value?.toString() ?? undefined
    })
  }

  protected setMetadataAttr(file: string, attribute: string, value: string): Promise<void> {
    return xattr.set(file, attribute, value)
  }
}
