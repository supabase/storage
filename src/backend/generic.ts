import { ObjectMetadata, ObjectResponse } from '../types/types'

export interface GetObjectHeaders {
  ifModifiedSince?: string
  ifNoneMatch?: string
  range?: string
}

export abstract class GenericStorageBackend {
  client: any
  constructor() {
    this.client = null
  }
  async getObject(
    bucketName: string,
    key: string,
    headers?: GetObjectHeaders
  ): Promise<ObjectResponse> {
    throw new Error('getObject not implemented')
  }
  async uploadObject(
    bucketName: string,
    key: string,
    body: NodeJS.ReadableStream,
    contentType: string,
    cacheControl: string
  ): Promise<ObjectMetadata> {
    throw new Error('uploadObject not implemented')
  }
  async deleteObject(bucket: string, key: string): Promise<ObjectMetadata> {
    throw new Error('deleteObject not implemented')
  }
  async copyObject(bucket: string, source: string, destination: string): Promise<ObjectMetadata> {
    throw new Error('copyObject not implemented')
  }
  async deleteObjects(bucket: string, prefixes: string[]): Promise<ObjectMetadata> {
    throw new Error('deleteObjects not implemented')
  }
  async headObject(bucket: string, key: string): Promise<ObjectMetadata> {
    throw new Error('headObject not implemented')
  }
}
