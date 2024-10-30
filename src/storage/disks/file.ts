import * as xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import fileChecksum from 'md5-file'
import { promisify } from 'util'
import stream from 'stream'
import MultiStream, { from } from 'multistream'
import {
  StorageDisk,
  ObjectMetadata,
  ObjectResponse,
  withOptionalVersion,
  UploadPart,
  CopyParams,
  ReadParams,
  SaveParams,
  DeleteParams,
  DeleteManyParams,
  MetadataParams,
  SignUrlParams,
  CreateMultiPartUploadParams,
  UploadPartParams,
  CompleteMultipartUploadParams,
  AbortMultipartUploadParams,
  UploadPartCopyParams,
  DiskOptions,
} from './disk'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { randomUUID } from 'crypto'
import fsExtra from 'fs-extra'
import { options } from 'axios'
import { bucket } from '../../http/routes'
import { contentType } from 'prom-client'
import fsExtra from 'fs-extra'
import fsExtra from 'fs-extra'
import fsExtra from 'fs-extra'
import { platform } from 'node:os'
import fsExtra from 'fs-extra'
import fsExtra from 'fs-extra'
import fsExtra from 'fs-extra'
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
export class FileSystemDisk implements StorageDisk {
  client = null
  mountPath: string
  prefix?: string

  constructor(options: DiskOptions) {
    if (!options.mountPath) {
      throw new Error('mount path not set')
    }
    this.mountPath = options.mountPath
    this.prefix = options.prefix
  }

  async read(params: ReadParams): Promise<ObjectResponse> {
    const { bucket, key, version, headers } = params
    const file = this.getKey(bucket, key, version)
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
          size: size,
          eTag: checksum,
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
          size: data.size,
          eTag: checksum,
          contentLength: fileSize,
        },
        body,
        httpStatusCode: 200,
      }
    }
  }

  async save(params: SaveParams): Promise<ObjectMetadata> {
    const { bucket, key, version, body, contentType, cacheControl } = params
    try {
      const file = this.getKey(bucket, key, version)
      await fs.ensureFile(file)
      const destFile = fs.createWriteStream(file)
      await pipeline(body, destFile)

      await this.setFileMetadata(file, { contentType, cacheControl })

      const metadata = await this.metadata({ bucket: bucket, key, version })

      return {
        ...metadata,
        httpStatusCode: 200,
      }
    } catch (err: any) {
      throw StorageBackendError.fromError(err)
    }
  }

  async delete(params: DeleteParams): Promise<void> {
    const { bucket, key, version } = params
    try {
      const file = this.getKey(bucket, key, version)
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

  async copy(
    params: CopyParams
  ): Promise<Pick<ObjectMetadata, 'httpStatusCode' | 'eTag' | 'lastModified'>> {
    const { to, from } = params

    const srcFile = this.getKey(from.bucket, from.key, from.version)
    const destFile = this.getKey(to.bucket, to.key, to.version)

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

  async deleteMany(params: DeleteManyParams): Promise<void> {
    const { bucket, keys } = params
    const promises = keys.map((prefix) => {
      return fs.rm(this.getKey(bucket, prefix))
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

  async metadata(params: MetadataParams): Promise<ObjectMetadata> {
    const { bucket, key, version } = params
    const file = this.getKey(bucket, key, version)

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

  async createMultiPartUpload(params: CreateMultiPartUploadParams): Promise<string | undefined> {
    const { bucket, key, version, contentType, cacheControl } = params
    const uploadId = randomUUID()
    const multiPartFolder = path.join(
      this.mountPath,
      'multiparts',
      uploadId,
      this.getKey(bucket, key, version)
    )

    const multipartFile = path.join(multiPartFolder, 'metadata.json')
    await fsExtra.ensureDir(multiPartFolder)
    await fsExtra.writeFile(multipartFile, JSON.stringify({ contentType, cacheControl }))

    return uploadId
  }

  async uploadPart(params: UploadPartParams): Promise<{ ETag?: string }> {
    const { bucket, key, version, uploadId, partNumber, body } = params
    const multiPartFolder = path.join(
      this.mountPath,
      'multiparts',
      uploadId,
      this.getKey(bucket, key, version)
    )

    const partPath = path.join(multiPartFolder, `part-${partNumber}`)

    const writeStream = fsExtra.createWriteStream(partPath)

    await pipeline(body as stream.Readable, writeStream)

    const etag = await fileChecksum(partPath)

    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await this.setMetadataAttr(partPath, METADATA_ATTR_KEYS[platform]['etag'], etag)

    return { ETag: etag }
  }

  async completeMultipartUpload(params: CompleteMultipartUploadParams): Promise<
    Omit<UploadPart, 'PartNumber'> & {
      location?: string
      bucket?: string
      version: string
    }
  > {
    const { bucket, key, uploadId, version, parts } = params
    const multiPartFolder = path.join(
      this.mountPath,
      'multiparts',
      uploadId,
      this.getKey(bucket, key, version)
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

    const uploaded = await this.save({
      bucket: bucket,
      key,
      version,
      body: multistream,
      contentType: metadata.contentType,
      cacheControl: metadata.cacheControl,
    })

    fsExtra.remove(path.join(this.mountPath, 'multiparts', uploadId)).catch(() => {
      // no-op
    })

    return {
      version: version,
      ETag: uploaded.eTag,
      bucket: bucket,
      location: `${bucket}/${key}`,
    }
  }

  async abortMultipartUpload(params: AbortMultipartUploadParams): Promise<void> {
    const { uploadId } = params
    const multiPartFolder = path.join(this.mountPath, 'multiparts', uploadId)

    await fsExtra.remove(multiPartFolder)
  }

  async uploadPartCopy(
    params: UploadPartCopyParams
  ): Promise<{ eTag?: string; lastModified?: Date }> {
    const { from, to, uploadId, partNumber, bytes } = params
    const multiPartFolder = path.join(
      this.mountPath,
      'multiparts',
      uploadId,
      this.getKey(to.bucket, to.key, to.version)
    )

    const partFilePath = path.join(multiPartFolder, `part-${partNumber}`)
    const sourceFilePath = path.join(
      this.mountPath,
      this.getKey(from.bucket, from.key, from.version)
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

  async signUrl(params: SignUrlParams): Promise<string> {
    const { bucket, key, version } = params
    return 'local:///' + this.getKey(bucket, key, version)
  }

  async setFileMetadata(file: string, { contentType, cacheControl }: FileMetadata) {
    const platform = process.platform == 'darwin' ? 'darwin' : 'linux'
    await Promise.all([
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['cache-control'], cacheControl),
      this.setMetadataAttr(file, METADATA_ATTR_KEYS[platform]['content-type'], contentType),
    ])
  }

  async close(): Promise<void> {
    return Promise.resolve(undefined)
  }

  protected getKey(bucket: string, key: string, version?: string) {
    const pathParts = [this.mountPath]

    if (this.prefix) {
      pathParts.push(this.prefix)
    }
    pathParts.push(withOptionalVersion(`${bucket}/${key}`, version))

    return pathParts.join('/')
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
