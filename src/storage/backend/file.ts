import xattr from 'fs-xattr'
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
  withPrefixAndVersion,
} from './generic'
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

export interface FileBackendOptions {
  bucket: string
  prefix?: string
}

/**
 * FileBackend
 * Interacts with the file system with this FileBackend adapter
 */
export class FileBackend implements StorageBackendAdapter {
  client = null
  filePath: string
  protected prefix?: string

  constructor(private readonly options: FileBackendOptions) {
    const { fileStoragePath } = getConfig()
    if (!fileStoragePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = fileStoragePath
    this.prefix = options.prefix
  }

  /**
   * Gets an object body and metadata
   * @param key
   * @param version
   */
  async getObject(key: string, version: string | undefined): Promise<ObjectResponse> {
    const file = path.resolve(
      this.filePath,
      withPrefixAndVersion(`${this.options.bucket}/${key}`, this.prefix, version)
    )
    const body = fs.createReadStream(file)
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
      const file = path.resolve(
        this.filePath,
        withPrefixAndVersion(`${this.options.bucket}/${key}`, this.prefix, version)
      )
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, {
        contentType,
        cacheControl,
      })

      const metadata = await this.headObject(key, version)

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
   * @param key
   * @param version
   */
  async deleteObject(key: string, version: string | undefined): Promise<void> {
    try {
      const file = path.resolve(
        this.filePath,
        withPrefixAndVersion(`${this.options.bucket}/${key}`, this.prefix, version)
      )
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
   * @param source
   * @param version
   * @param destination
   * @param destinationVersion
   */
  async copyObject(
    source: string,
    version: string | undefined,
    destination: string,
    destinationVersion: string
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode'>> {
    const srcFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${this.options.bucket}/${source}`, version)
    )
    const destFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${this.options.bucket}/${destination}`, destinationVersion)
    )

    await fs.ensureFile(destFile)
    await fs.copyFile(srcFile, destFile)

    await this.setFileMetadata(destFile, await this.getFileMetadata(srcFile))

    return {
      httpStatusCode: 200,
    }
  }

  /**
   * Deletes multiple objects
   * @param prefixes
   */
  async deleteObjects(prefixes: string[]): Promise<void> {
    const promises = prefixes.map((prefix) => {
      return fs.rm(
        path.resolve(
          this.filePath,
          withPrefixAndVersion(path.join(this.options.bucket, prefix), this.prefix)
        )
      )
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
   * @param key
   * @param version
   */
  async headObject(key: string, version: string | undefined): Promise<ObjectMetadata> {
    const file = path.join(
      this.filePath,
      withPrefixAndVersion(`${this.options.bucket}/${key}`, this.prefix, version)
    )

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
   * @param key
   * @param version
   */
  async privateAssetUrl(key: string, version: string | undefined): Promise<string> {
    return (
      'local:///' +
      path.join(
        this.filePath,
        withPrefixAndVersion(`${this.options.bucket}/${key}`, this.prefix, version)
      )
    )
  }

  async setFileMetadata(file: string, { contentType, cacheControl }: FileMetadata) {
    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await Promise.all([
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['cache-control'], cacheControl),
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['content-type'], contentType),
    ])
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
    return xattr.get(file, attribute).then((value) => {
      return value?.toString() ?? undefined
    })
  }

  protected setMetadataAttr(file: string, attribute: string, value: string): Promise<void> {
    return xattr.set(file, attribute, value)
  }
}
