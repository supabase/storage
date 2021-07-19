import { ObjectMetadata, ObjectResponse } from '../types/types'
import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import stream from 'stream'
import { getConfig } from '../utils/config'
import { GenericStorageBackend } from './generic'
const pipeline = promisify(stream.pipeline)

export class FileBackend implements GenericStorageBackend {
  client: null
  filePath: string

  constructor() {
    const { fileStoragePath } = getConfig()
    if (!fileStoragePath) {
      throw new Error('FILE_STORAGE_BACKEND_PATH env variable not set')
    }
    this.filePath = fileStoragePath
  }

  async getObject(bucketName: string, key: string, range?: string): Promise<ObjectResponse> {
    const file = path.resolve(this.filePath, `${bucketName}/${key}`)
    const body = await fs.readFile(file)
    const data = await fs.stat(file)
    const lastModified = new Date(0)
    lastModified.setUTCMilliseconds(data.mtimeMs)
    return {
      metadata: {
        // cacheControl: data.CacheControl,
        // mimetype: data.ContentType,
        // eTag: data.ETag,
        lastModified: lastModified,
        // contentRange: data.ContentRange,
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
    const file = path.resolve(this.filePath, `${bucketName}/${key}`)
    await fs.ensureFile(file)
    const destFile = fs.createWriteStream(file)
    await pipeline(body, destFile)
    return {
      httpStatusCode: 200,
    }
  }

  async deleteObject(bucket: string, key: string): Promise<ObjectMetadata> {
    const file = path.resolve(this.filePath, `${bucket}/${key}`)
    await fs.rm(file)
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
