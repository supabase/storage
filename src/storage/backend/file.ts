import xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import { getConfig } from '../../config'
import { StorageBackendAdapter, ObjectMetadata, ObjectResponse } from './generic'
import { StorageBackendError } from '../errors'
const pipeline = promisify(stream.pipeline)

interface FileMetadata {
  cacheControl: string
  contentType: string
}

/**
 * FileBackend
 * Interacts with the file system with this FileBackend adapter
 */
export class FileBackend implements StorageBackendAdapter {
  client = null
  filePath: string

  constructor() {
    const { fileStoragePath } = getConfig()
    if (!fileStoragePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = fileStoragePath
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   */
  async getObject(bucketName: string, key: string): Promise<ObjectResponse> {
    const file = path.resolve(this.filePath, `${bucketName}/${key}`)
    const body = await fs.readFile(file)
    const data = await fs.stat(file)
    const { cacheControl, contentType } = await this.getFileMetadata(file)
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)

    const checksum = await fileChecksum(file)

    return {
      metadata: {
        cacheControl: cacheControl || 'no-cache',
        mimetype: contentType || 'application/octet-stream',
        lastModified: lastModified,
        // contentRange: data.ContentRange, @todo: support range requests
        httpStatusCode: 200,
        size: data.size,
        eTag: checksum,
        contentLength: data.size,
      },
      body,
    }
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
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    try {
      const file = path.resolve(this.filePath, `${bucketName}/${key}`)
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, {
        contentType,
        cacheControl,
      })

      const metadata = await this.headObject(bucketName, key)

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
   */
  async deleteObject(bucket: string, key: string): Promise<void> {
    const file = path.resolve(this.filePath, `${bucket}/${key}`)
    await fs.remove(file)
  }

  /**
   * Copies an existing object to the given location
   * @param bucket
   * @param source
   * @param destination
   */
  async copyObject(
    bucket: string,
    source: string,
    destination: string
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
    const srcFile = path.resolve(this.filePath, `${bucket}/${source}`)
    const destFile = path.resolve(this.filePath, `${bucket}/${destination}`)

    await fs.copyFile(srcFile, destFile)

    await this.setFileMetadata(destFile, await this.getFileMetadata(srcFile))

    return {
      httpStatusCode: 200,
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
    await Promise.all(promises)
  }

  /**
   * Returns metadata information of a specific object
   * @param bucket
   * @param key
   */
  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const file = path.resolve(this.filePath, `${bucket}/${key}`)
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

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   */
  async privateAssetUrl(bucket: string, key: string): Promise<string> {
    return 'local:///' + path.join(this.filePath, `${bucket}/${key}`)
  }

  protected async getFileMetadata(file: string) {
    const [cacheControl, contentType] = await Promise.all([
      this.getMetadataAttr(file, 'user.supabase.cache-control'),
      this.getMetadataAttr(file, 'user.supabase.content-type'),
    ])

    return {
      cacheControl,
      contentType,
    } as FileMetadata
  }

  protected async setFileMetadata(file: string, { contentType, cacheControl }: FileMetadata) {
    await Promise.all([
      this.setMetadataAttr(file, 'user.supabase.content-type', contentType),
      this.setMetadataAttr(file, 'user.supabase.cache-control', cacheControl),
    ])
  }

  protected getMetadataAttr(file: string, attribute: string): Promise<string | undefined> {
    return xattr.get(file, attribute).then((value) => {
      return value?.toString() ?? undefined
    })
  }

  protected setMetadataAttr(file: string, attribute: string, value: string): Promise<void> {
    return xattr.set(file, attribute, value)
  }
}
