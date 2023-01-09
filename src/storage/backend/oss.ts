import OSS from 'ali-oss'
import { StorageBackendError } from '../errors'
import {
  StorageBackendAdapter,
  ObjectMetadata,
  ObjectResponse,
  BrowserCacheHeaders,
} from './generic'

interface Headers {
  'content-length': number
  'content-type': string
  'cache-control': string
  'last-modified': Date
  'content-range': string
  etag: string
}

export class OSSBackend implements StorageBackendAdapter {
  client: OSS

  constructor(
    endpoint: string | undefined,
    accessKey: string,
    accessSecret: string,
    timeout?: number
  ) {
    const params: OSS.Options = {
      endpoint,
      accessKeyId: accessKey,
      accessKeySecret: accessSecret,
      timeout: timeout ?? 300000,
    }
    this.client = new OSS(params)
  }

  /**
   * Gets an object body and metadata
   * @param bucketName
   * @param key
   * @param headers
   */
  async getObject(
    bucketName: string,
    key: string,
    headers?: BrowserCacheHeaders
  ): Promise<ObjectResponse> {
    this.client.useBucket(bucketName)
    const data = await this.client.get(key)

    return {
      metadata: {
        cacheControl: (data.res.headers as Headers)['cache-control'] || 'no-cache',
        mimetype: (data.res.headers as Headers)['content-type'] || 'application/octa-stream',
        eTag: (data.res.headers as Headers)['etag'] || '',
        lastModified: (data.res.headers as Headers)['last-modified'],
        contentRange: (data.res.headers as Headers)['content-range'],
        contentLength: (data.res.headers as Headers)['content-length'] || 0,
        httpStatusCode: data.res.status || 200,
        size: (data.res.headers as Headers)['content-length'],
      },
      body: data.content,
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
      this.client.useBucket(bucketName)
      await this.client.putStream(key, body)
      const data = await this.client.head(key, {
        headers: { 'Content-Type': contentType, 'Cache-Control': cacheControl },
      })

      return {
        httpStatusCode: data.res.status || 200,
        cacheControl: (data.res.headers as Headers)['cache-control'],
        eTag: (data.res.headers as Headers).etag,
        mimetype: (data.res.headers as Headers)['content-type'],
        contentLength: (data.res.headers as Headers)['content-length'],
        lastModified: (data.res.headers as Headers)['last-modified'],
        size: (data.res.headers as Headers)['content-length'],
        contentRange: (data.res.headers as Headers)['content-range'],
      }
    } catch (err) {
      throw StorageBackendError.fromError(err)
    }
  }

  /**
   * Deletes an object
   * @param bucket
   * @param key
   */
  async deleteObject(bucket: string, key: string): Promise<void> {
    this.client.useBucket(bucket)
    await this.client.delete(key)
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
    this.client.useBucket(bucket)
    try {
      const data = await this.client.copy(destination, source)
      return {
        httpStatusCode: data.res.status || 200,
      }
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Deletes multiple objects
   * @param bucket
   * @param prefixes
   */
  async deleteObjects(bucket: string, prefixes: string[]): Promise<void> {
    this.client.useBucket(bucket)
    try {
      await this.client.deleteMulti(prefixes)
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns metadata information of a specific object
   * @param bucket
   * @param key
   */
  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    this.client.useBucket(bucket)
    try {
      const data = await this.client.head(key)
      return {
        cacheControl: (data.res.headers as Headers)['cache-control'] || 'no-cache',
        mimetype: (data.res.headers as Headers)['content-type'] || 'application/octa-stream',
        eTag: (data.res.headers as Headers)['etag'] || '',
        lastModified: (data.res.headers as Headers)['last-modified'],
        contentLength: (data.res.headers as Headers)['content-length'] || 0,
        httpStatusCode: data.res.status || 200,
        size: (data.res.headers as Headers)['content-length'],
      }
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }

  /**
   * Returns a private url that can only be accessed internally by the system
   * @param bucket
   * @param key
   */
  async privateAssetUrl(bucket: string, key: string): Promise<string> {
    this.client.useBucket(bucket)
    // expires use second as unit
    try {
      return this.client.signatureUrl(key, { expires: 600 })
    } catch (e) {
      throw StorageBackendError.fromError(e)
    }
  }
}
