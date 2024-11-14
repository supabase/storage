import * as xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import MultiStream from 'multistream'
import {
  StorageDisk,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  ReadParams,
  UploadObjectParams,
  DeleteObjectParams,
  CopyObjectParams,
  DeleteObjectsParams,
  HeadObjectParams,
  PrivateAssetUrlParams,
  CreateMultiPartUploadParams,
  UploadPartParams,
  CompleteMultipartUploadParams,
  AbortMultipartUploadParams,
  UploadPartCopyParams,
  DiskAdapterOptions,
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
export class FileDisk implements StorageDisk {
  filePath: string

  constructor(protected readonly options: DiskAdapterOptions) {
    this.filePath = options.prefix
      ? path.join(options.prefix, options.mountPoint)
      : options.mountPoint
  }

  withPrefix(prefix: string): StorageDisk {
    return new FileDisk({ ...this.options, prefix })
  }

  async read({ bucketName, key, version, headers }: ReadParams): Promise<ObjectResponse> {
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
          lastModified,
          contentRange: `bytes ${startRange}-${endRange}/${fileSize}`,
          httpStatusCode: 206,
          size,
          eTag: checksum,
          contentLength: chunkSize,
        },
        httpStatusCode: 206,
        body,
      }
    }

    const body = fs.createReadStream(file)
    return {
      metadata: {
        cacheControl: cacheControl || 'no-cache',
        mimetype: contentType || 'application/octet-stream',
        lastModified,
        httpStatusCode: 200,
        size: data.size,
        eTag: checksum,
        contentLength: fileSize,
      },
      body,
      httpStatusCode: 200,
    }
  }

  async save({
    bucketName,
    key,
    version,
    body,
    contentType,
    cacheControl,
  }: UploadObjectParams): Promise<ObjectMetadata> {
    try {
      const file = path.resolve(this.filePath, withOptionalVersion(`${bucketName}/${key}`, version))
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, { contentType, cacheControl })

      const metadata = await this.info({ bucket: bucketName, key, version })
      return {
        ...metadata,
        httpStatusCode: 200,
      }
    } catch (err: unknown) {
      throw StorageBackendError.fromError(err)
    }
  }

  async delete({ bucket, key, version }: DeleteObjectParams): Promise<void> {
    try {
      const file = path.resolve(this.filePath, withOptionalVersion(`${bucket}/${key}`, version))
      await fs.remove(file)
    } catch (e) {
      if (e instanceof Error && 'code' in e && (e as any).code === 'ENOENT') {
        return
      }
      throw e
    }
  }

  async copy({
    source,
    destination,
    metadata,
  }: CopyObjectParams): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const srcFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${source.bucket}/${source.key}`, source.version)
    )
    const destFile = path.resolve(
      this.filePath,
      withOptionalVersion(`${destination.bucket}/${destination.key}`, destination.version)
    )

    await fs.ensureFile(destFile)
    await fs.copyFile(srcFile, destFile)

    const originalMetadata = await this.getFileMetadata(srcFile)
    await this.setFileMetadata(destFile, { ...originalMetadata, ...metadata })

    const fileStat = await fs.lstat(destFile)
    const checksum = await fileChecksum(destFile)

    return {
      httpStatusCode: 200,
      lastModified: fileStat.mtime,
      eTag: checksum,
    }
  }

  async deleteMany({ bucket, keys }: DeleteObjectsParams): Promise<void> {
    const promises = keys.map((key) => {
      return fs.rm(path.resolve(this.filePath, bucket, key))
    })
    const results = await Promise.allSettled(promises)

    results.forEach((result) => {
      if (result.status === 'rejected' && result.reason.code !== 'ENOENT') {
        throw result.reason
      }
    })
  }

  async info({ bucket, key, version }: HeadObjectParams): Promise<ObjectMetadata> {
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

  async privateAssetUrl({ bucket, key, version }: PrivateAssetUrlParams): Promise<string> {
    return 'local:///' + path.join(this.filePath, withOptionalVersion(`${bucket}/${key}`, version))
  }

  async createMultiPartUpload({
    bucketName,
    key,
    version,
    contentType,
    cacheControl,
  }: CreateMultiPartUploadParams): Promise<string | undefined> {
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

  async uploadPart({
    bucketName,
    key,
    version,
    uploadId,
    partNumber,
    body,
  }: UploadPartParams): Promise<{ ETag?: string }> {
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      uploadId,
      bucketName,
      withOptionalVersion(key, version)
    )

    const partPath = path.join(multiPartFolder, `part-${partNumber}`)
    const writeStream = fsExtra.createWriteStream(partPath)

    if (!body) throw new Error('Body is required')
    await pipeline(body, writeStream)

    const etag = await fileChecksum(partPath)
    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await this.setMetadataAttr(partPath, METADATA_ATTR_KEYS[platform]['etag'], etag)

    return { ETag: etag }
  }

  async completeMultipartUpload({
    bucketName,
    key,
    uploadId,
    version,
    parts,
  }: CompleteMultipartUploadParams): Promise<
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

    const fileStreams = finalParts.map((partPath) => fs.createReadStream(partPath))
    const multistream = new MultiStream(fileStreams)
    const metadataContent = await fsExtra.readFile(
      path.join(multiPartFolder, 'metadata.json'),
      'utf-8'
    )
    const metadata = JSON.parse(metadataContent)

    const uploaded = await this.save({
      bucketName,
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
      version,
      ETag: uploaded.eTag,
      bucket: bucketName,
      location: `${bucketName}/${key}`,
    }
  }

  async abortMultipartUpload({
    bucketName,
    key,
    uploadId,
    version,
  }: AbortMultipartUploadParams): Promise<void> {
    const multiPartFolder = path.join(this.filePath, 'multiparts', uploadId)
    await fsExtra.remove(multiPartFolder)
  }

  async uploadPartCopy({
    UploadId,
    PartNumber,
    bytes,
    source,
    destination,
  }: UploadPartCopyParams): Promise<{ eTag?: string; lastModified?: Date }> {
    const multiPartFolder = path.join(
      this.filePath,
      'multiparts',
      UploadId,
      destination.bucket,
      withOptionalVersion(destination.key, destination.version)
    )

    const partFilePath = path.join(multiPartFolder, `part-${PartNumber}`)
    const sourceFilePath = path.join(
      this.filePath,
      source.bucket,
      withOptionalVersion(source.key, source.version)
    )

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    const readStreamOptions = bytes ? { start: bytes.fromByte, end: bytes.toByte } : {}
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
