import * as xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import { getConfig } from '../../config'
import {
  StorageBackendAdapter,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  BrowserCacheHeaders,
  UploadPart,
} from './adapter'
import { StorageBackendError } from '../errors'
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
  },
  linux: {
    'cache-control': 'user.supabase.cache-control',
    'content-type': 'user.supabase.content-type',
  },
}

/**
 * FileBackend
 * Interacts with the file system with this FileBackend adapter
 */
export class FileBackend implements StorageBackendAdapter {
  client = null
  filePath: string

  constructor() {
    const { storageFilePath } = getConfig()
    if (!storageFilePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = storageFilePath
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
    const checksum = await fileChecksum(file)
    const fileSize = data.size
    const { cacheControl, contentType } = await this.getFileMetadata(file)
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)

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
          eTag: checksum,
          contentLength: chunkSize,
        },
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
          eTag: checksum,
          contentLength: fileSize,
        },
        body,
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
   */
  async copyObject(
    bucket: string,
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const srcFile = path.resolve(this.filePath, withOptionalVersion(`${bucket}/${source}`, version))
    const destFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${bucket}/${destination}`, destinationVersion)
    )

    await fs.ensureFile(destFile)
    await fs.copyFile(srcFile, destFile)

    await this.setFileMetadata(destFile, await this.getFileMetadata(srcFile))

    const fileStat = await fs.lstat(destFile)
    const checksum = await fileChecksum(destFile)

    return {
      httpStatusCode: 200,
      lastModified: fileStat.mtime,
      eTag: checksum,
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

    const checksum = await fileChecksum(file)

    return {
      httpStatusCode: 200,
      size: data.size,
      cacheControl: cacheControl || 'no-cache',
      mimetype: contentType || 'application/octet-stream',
      eTag: `"${checksum}"`,
      lastModified: data.birthtime,
      contentLength: data.size,
    }
  }

  createMultiPartUpload(
    bucketName: string,
    key: string,
    version: string | undefined,
    contentType: string,
    cacheControl: string
  ): Promise<string | undefined> {
    throw new Error('Method not implemented.')
  }

  async uploadPart(
    bucketName: string,
    key: string,
    version: string,
    uploadId: string,
    partNumber: number,
    body: stream.Readable
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

  async abortMultipartUpload(bucketName: string, key: string, uploadId: string): Promise<void> {
    return Promise.resolve(undefined)
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
