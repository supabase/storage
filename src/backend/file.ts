import { ObjectMetadata, ObjectResponse } from '../types/types'
import xattr from 'fs-xattr'
import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import stream from 'stream'
import { getConfig } from '../utils/config'
import { GenericStorageBackend } from './generic'
import { convertErrorToStorageBackendError } from '../utils/errors'
const pipeline = promisify(stream.pipeline)

export class FileBackend implements GenericStorageBackend {
  client = null
  filePath: string

  constructor() {
    const { fileStoragePath } = getConfig()
    if (!fileStoragePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = fileStoragePath
  }

  getMetadata(file: string, attribute: string): Promise<string | undefined> {
    return xattr.get(file, attribute).then((value) => {
      return value?.toString() ?? undefined
    })
  }

  setMetadata(file: string, attribute: string, value: string): Promise<void> {
    return xattr.set(file, attribute, value)
  }

  async getObject(bucketName: string, key: string): Promise<ObjectResponse> {
    const file = path.resolve(this.filePath, `${bucketName}/${key}`)
    const body = await fs.readFile(file)
    const data = await fs.stat(file)
    const cacheControl = await this.getMetadata(file, 'user.supabase.cache-control')
    const contentType = await this.getMetadata(file, 'user.supabase.content-type')
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)
    return {
      metadata: {
        cacheControl,
        mimetype: contentType,
        lastModified: lastModified,
        // contentRange: data.ContentRange, @todo: support range requests
        httpStatusCode: 200,
      },
      body,
    }
  }

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
      await Promise.all([
        this.setMetadata(file, 'user.supabase.content-type', contentType),
        this.setMetadata(file, 'user.supabase.cache-control', cacheControl),
      ])
      return {
        httpStatusCode: 200,
      }
    } catch (err: any) {
      throw convertErrorToStorageBackendError(err)
    }
  }

  async deleteObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const file = path.resolve(this.filePath, `${bucket}/${key}`)
    await fs.remove(file)
    return {}
  }

  async copyObject(bucket: string, source: string, destination: string): Promise<ObjectMetadata> {
    const srcFile = path.resolve(this.filePath, `${bucket}/${source}`)
    const destFile = path.resolve(this.filePath, `${bucket}/${destination}`)
    await fs.copyFile(srcFile, destFile)
    return {
      httpStatusCode: 200,
    }
  }

  async deleteObjects(bucket: string, prefixes: string[]): Promise<ObjectMetadata> {
    const promises = prefixes.map((prefix) => {
      return fs.rm(path.resolve(this.filePath, bucket, prefix))
    })
    await Promise.all(promises)
    return {}
  }

  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const file = path.resolve(this.filePath, `${bucket}/${key}`)
    const data = await fs.stat(file)
    return {
      httpStatusCode: 200,
      size: data.size,
    }
  }
}
