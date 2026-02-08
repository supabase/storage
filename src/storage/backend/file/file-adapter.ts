import * as xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import MultiStream from 'multistream'
import { getConfig } from '../../../config'
import {
  StorageBackendAdapter,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
  ListObjectsInput,
  ReadObjectInput,
  WriteObjectInput,
  RemoveObjectInput,
  CopyObjectInput,
  RemoveManyObjectsInput,
  StatsObjectInput,
  TempPrivateAccessUrlInput,
  CreateMultiPartUploadInput,
  UploadPartInput,
  CompleteMultipartUploadInput,
  AbortMultipartUploadInput,
  UploadPartCopyInput,
} from '../adapter'
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
    etag: 'user.supabase.etag',
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
    this.filePath = path.isAbsolute(storageFilePath)
      ? storageFilePath
      : path.resolve(__dirname, '..', '..', '..', storageFilePath)
    this.etagAlgorithm = storageFileEtagAlgorithm
  }

  async list(
    input: ListObjectsInput
  ): Promise<{ keys: { name: string; size: number }[]; nextToken?: string }> {
    return Promise.resolve({ keys: [] })
  }

  /**
   * Gets an object body and metadata
   */
  async read(input: ReadObjectInput): Promise<ObjectResponse> {
    const { bucket, key, version, headers } = input
    // 'Range: bytes=#######-######
    const file = this.resolveSecurePath(withOptionalVersion(`${bucket}/${key}`, version))
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
   */
  async write(input: WriteObjectInput): Promise<ObjectMetadata> {
    const { bucket, key, version, body, contentType, cacheControl } = input
    try {
      const file = this.resolveSecurePath(withOptionalVersion(`${bucket}/${key}`, version))
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, {
        contentType: contentType || 'application/octet-stream',
        cacheControl: cacheControl || 'no-cache',
      })

      const metadata = await this.stats({ bucket, key, version })

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
   */
  async remove(input: RemoveObjectInput): Promise<void> {
    const { bucket, key, version } = input
    try {
      const file = this.resolveSecurePath(withOptionalVersion(`${bucket}/${key}`, version))
      await fs.remove(file)

      // Clean up empty parent directories
      await this.cleanupEmptyDirectories(path.dirname(file))
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
   */
  async copy(
    input: CopyObjectInput
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const { bucket, source, version, destination, destinationVersion, metadata } = input
    const srcFile = this.resolveSecurePath(withOptionalVersion(`${bucket}/${source}`, version))
    const destFile = this.resolveSecurePath(
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
   */
  async removeMany(input: RemoveManyObjectsInput): Promise<void> {
    const { bucket, prefixes } = input
    const promises = prefixes.map((prefix) => {
      return fs.rm(this.resolveSecurePath(`${bucket}/${prefix}`))
    })
    const results = await Promise.allSettled(promises)

    // Collect unique parent directories for cleanup
    const parentDirs = new Set<string>()

    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        if (result.reason.code === 'ENOENT') {
          return
        }
        throw result.reason
      } else {
        // Add parent directory of successfully deleted file
        const filePath = this.resolveSecurePath(`${bucket}/${prefixes[index]}`)
        parentDirs.add(path.dirname(filePath))
      }
    })

    // Clean up empty directories
    for (const dir of parentDirs) {
      try {
        await this.cleanupEmptyDirectories(dir)
      } catch {
        // Ignore cleanup errors to not affect the main deletion operation
      }
    }
  }

  /**
   * Returns metadata information of a specific object
   */
  async stats(input: StatsObjectInput): Promise<ObjectMetadata> {
    const { bucket, key, version } = input
    const file = this.resolveSecurePath(withOptionalVersion(`${bucket}/${key}`, version))

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

  async createMultiPartUpload(input: CreateMultiPartUploadInput): Promise<string | undefined> {
    const { bucket, key, version, contentType, cacheControl } = input
    const uploadId = randomUUID()
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucket,
      withOptionalVersion(key, version)
    )

    const multipartFile = path.join(multiPartFolder, 'metadata.json')
    await fsExtra.ensureDir(multiPartFolder)
    await fsExtra.writeFile(multipartFile, JSON.stringify({ contentType, cacheControl }))

    return uploadId
  }

  async uploadPart(input: UploadPartInput): Promise<{ ETag?: string }> {
    const { bucket, key, version, uploadId, partNumber, body } = input
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucket,
      withOptionalVersion(key, version)
    )

    const partPath = path.join(multiPartFolder, `part-${partNumber}`)

    const writeStream = fsExtra.createWriteStream(partPath)

    await pipeline(body as stream.Readable, writeStream)

    const etag = await fileChecksum(partPath)

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await this.setMetadataAttr(partPath, METADATA_ATTR_KEYS[platform]['etag'], etag)

    return { ETag: etag }
  }

  async completeMultipartUpload(input: CompleteMultipartUploadInput): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    const { bucket, key, uploadId, version, parts } = input
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucket,
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

    const uploaded = await this.write({
      bucket,
      key,
      version,
      body: multistream,
      contentType: metadata.contentType,
      cacheControl: metadata.cacheControl,
    })

    fsExtra.remove(path.join(this.filePath, 'multiparts', uploadId)).catch(() => {
      // no-op
    })

    return {
      version: version,
      ETag: uploaded.eTag,
      bucket: bucket,
      location: `${bucket}/${key}`,
    }
  }

  async abortMultipartUpload(input: AbortMultipartUploadInput): Promise<void> {
    const { uploadId } = input
    const multiPartFolder = path.join(this.filePath, 'multiparts', uploadId)

    await fsExtra.remove(multiPartFolder)

    // Clean up empty parent directories
    try {
      await this.cleanupEmptyDirectories(path.dirname(multiPartFolder))
    } catch {
      // Ignore cleanup errors
    }
  }

  async uploadPartCopy(
    input: UploadPartCopyInput
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    const { bucket, key, version, uploadId, partNumber, sourceKey, sourceKeyVersion, bytesRange } =
      input
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucket,
      withOptionalVersion(key, version)
    )

    const partFilePath = path.join(multiPartFolder, `part-${partNumber}`)
    const sourceFilePath = this.resolveSecurePath(
      `${bucket}/${withOptionalVersion(sourceKey, sourceKeyVersion)}`
    )

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'

    const readStreamOptions = bytesRange
      ? { start: bytesRange.fromByte, end: bytesRange.toByte }
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
   */
  async tempPrivateAccessUrl(input: TempPrivateAccessUrlInput): Promise<string> {
    const { bucket, key, version } = input
    return 'local:///' + this.resolveSecurePath(withOptionalVersion(`${bucket}/${key}`, version))
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
    return xattr.get(file, attribute).then((value) => {
      return value?.toString() ?? undefined
    })
  }

  protected setMetadataAttr(file: string, attribute: string, value: string): Promise<void> {
    return xattr.set(file, attribute, value)
  }

  /**
   * Efficiently checks if a directory is empty by reading only the first entry
   * @param dirPath The directory path to check
   * @returns Promise<boolean> true if directory is empty, false otherwise
   */
  protected async isEmptyDirectory(dirPath: string): Promise<boolean> {
    try {
      const directory = await fs.opendir(dirPath)
      const entry = await directory.read()
      await directory.close()

      return entry === null
    } catch {
      return false
    }
  }

  /**
   * Recursively removes empty directories up to the storage root
   * @param dirPath The directory path to start cleanup from
   */
  protected async cleanupEmptyDirectories(dirPath: string): Promise<void> {
    try {
      // Don't cleanup beyond the storage root path
      if (!dirPath.startsWith(this.filePath) || dirPath === this.filePath) {
        return
      }

      // Check if directory exists
      const exists = await fs.pathExists(dirPath)
      if (!exists) {
        return
      }

      // Check if directory is empty - using opendir for better performance with large directories
      const isEmpty = await this.isEmptyDirectory(dirPath)
      if (isEmpty) {
        // Remove empty directory - using fs.remove for better cross-platform compatibility
        await fs.remove(dirPath)

        // Recursively check parent directory
        const parentDir = path.dirname(dirPath)
        await this.cleanupEmptyDirectories(parentDir)
      }
    } catch {
      // Ignore errors during cleanup to not affect main operations
    }
  }

  /**
   * Securely resolves a path within the storage directory, preventing path traversal attacks
   * @param relativePath The relative path to resolve
   * @throws {StorageBackendError} If the resolved path escapes the storage directory
   */
  private resolveSecurePath(relativePath: string): string {
    const resolvedPath = path.resolve(this.filePath, relativePath)
    const normalizedPath = path.normalize(resolvedPath)

    // Ensure the resolved path is within the storage directory
    if (!normalizedPath.startsWith(this.filePath + path.sep) && normalizedPath !== this.filePath) {
      throw ERRORS.InvalidKey(
        `Path traversal detected: ${relativePath} resolves outside storage directory`
      )
    }

    return normalizedPath
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
}
