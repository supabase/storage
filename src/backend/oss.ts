import { ObjectMetadata, ObjectResponse } from '../types/types'
import { GenericStorageBackend } from './generic'
import OSS from 'ali-oss'

export class OSSBackend implements GenericStorageBackend {
  client: OSS

  constructor(
    bucket: string,
    endpoint: string,
    accessKeyId: string,
    accessKeySecret: string,
    timeout?: number
  ) {
    const params: OSS.Options = {
      bucket,
      endpoint,
      accessKeyId,
      accessKeySecret,
      timeout, // default 6000ms
    }
    this.client = new OSS(params)
  }

  async getObject(bucketName: string, key: string, range?: string): Promise<ObjectResponse> {
    console.log(`range: ${range}`)
    console.log(bucketName)
    const data = await this.client.get(key)
    return {
      metadata: {
        // cacheControl: data.res.headers.resCacheControl,
        // mimetype: data.res.headers.ContentType,
        // eTag: data.res.headers.ETag,
        // lastModified: data.res.headers.LastModified,
        // contentRange: data.res.headers.ContentRange,
        size: data.res.size,
        httpStatusCode: data.res.status,
      },
      body: data.content,
    }
  }

  async uploadObject(
    bucketName: string,
    key: string,
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    console.log(`contentType: ${contentType}, cacheControl: ${cacheControl}`)
    console.log(bucketName)
    const data = await this.client.putStream(key, body)
    return {
      httpStatusCode: data.res.status,
    }
  }

  async deleteObject(bucket: string, key: string): Promise<ObjectMetadata> {
    console.log(bucket)
    console.log(key)
    await this.client.delete(key)
    return {}
  }

  async copyObject(bucket: string, source: string, destination: string): Promise<ObjectMetadata> {
    console.log(bucket)
    const data = await this.client.copy(destination, source)
    return {
      httpStatusCode: data.res.status,
    }
  }

  async deleteObjects(bucket: string, prefixes: string[]): Promise<ObjectMetadata> {
    console.log(bucket)
    console.log(prefixes)
    await this.client.deleteMulti(prefixes)
    return {}
  }

  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    console.log(bucket)
    const data = await this.client.head(key)
    return {
      httpStatusCode: data.status,
      size: data.res.size,
    }
  }
}
