import { RequestGenericInterface } from 'fastify'

interface genericObjectRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
}

interface genericBucketRequest extends RequestGenericInterface {
  Params: {
    bucketId: string
    '*': string
  }
}

interface bucketCreateRequest extends RequestGenericInterface {
  Body: {
    name: string
  }
}

interface signRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
  Body: {
    expiresIn: number
  }
}

interface getSignedObjectRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
  Querystring: {
    token: string
  }
}

interface copyRequest extends RequestGenericInterface {
  Body: {
    sourceKey: string
    bucketName: string
    destinationKey: string
  }
}

interface deleteObjectsRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
  }
  Body: {
    prefixes: string[]
  }
}

interface searchRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
  }
  Body: {
    prefix: string
    limit: number
    offset: number
  }
}

type Bucket = {
  id: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
}

type Obj = {
  id: string
  bucketId: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
  lastAccessedAt: string
  metadata?: Record<string, unknown>
  buckets?: Bucket
}

type signedToken = {
  url: string
}
