import { S3ProtocolHandler } from './handler'

export type HTTPMethod = 'get' | 'put' | 'post' | 'head' | 'delete' | 'patch'

type ResponseType = {
  statusCode?: number
  headers?: Record<string, string>
  responseBody?: unknown
}
type Handler = (s3Protocol: S3ProtocolHandler) => Promise<ResponseType>

export interface EndpointSchema<
  Params = unknown,
  Headers = unknown,
  Query = unknown,
  Body = unknown
> {
  url: string
  method: HTTPMethod | string
  body?: Body
  query?: Query
  params?: Params
  headers?: Headers
  response?: unknown
  raw?: ReadableStream
}

export class Router<
  Req extends EndpointSchema<
    Record<string, any>,
    Record<string, any>,
    Record<string, any>,
    Record<string, any>
  >
> {
  routes() {
    const routes: Record<string, (req: Req) => Handler | undefined> = {
      '/': this.handleRoot.bind(this),
      '/:Bucket/': this.handleBuckets.bind(this),
      '/:Bucket/*': this.handleObjects.bind(this),
    }

    return routes
  }

  protected handleRoot(req: Req) {
    switch (req.method.toLowerCase()) {
      case 'get':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) => s3Protocol.listBuckets(),
        })
    }
  }

  /**
   * Handles Buckets actions
   * @param req
   * @protected
   */
  protected handleBuckets(req: Req) {
    switch (req.method.toLowerCase()) {
      case 'get':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) =>
            s3Protocol.listObjectsV2({
              Bucket: this.getParam(req, 'Bucket'),
              Prefix: req.query?.prefix || '',
              ContinuationToken: req.query?.['continuation-token'],
              StartAfter: req.query?.['start-after'],
              EncodingType: req.query?.['encoding-type'],
              MaxKeys: req.query?.['max-keys'],
              Delimiter: req.query?.delimiter,
            }),
        })
      case 'put':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) =>
            s3Protocol.createBucket(
              this.getParam(req, 'Bucket'),
              req.headers?.['x-amz-acl'] === 'public-read'
            ),
        })
      case 'delete':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) => s3Protocol.deleteBucket(this.getParam(req, 'Bucket')),
        })
    }
  }

  /**
   * Handles Objects actions
   * @param req
   * @protected
   */
  protected handleObjects(req: Req) {
    switch (req.method.toLowerCase()) {
      case 'get':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) =>
            s3Protocol.getObject({
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
              Range: req.headers?.['range'],
              IfNoneMatch: req.headers?.['if-none-match'],
              IfModifiedSince: req.headers?.['if-modified-since'],
            }),
        })
      case 'post':
        return this.matchQueryString(req.query, {
          uploadId: (s3Protocol) =>
            s3Protocol.completeMultiPartUpload({
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
              UploadId: req.query?.uploadId,
              MultipartUpload: req.body?.CompleteMultipartUpload,
            }),
          uploads: (s3Protocol) =>
            s3Protocol.createMultiPartUpload({
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
              ContentType: req.headers?.['content-type'],
              CacheControl: req.headers?.['cache-control'],
              ContentDisposition: req.headers?.['content-disposition'],
              ContentEncoding: req.headers?.['content-encoding'],
            }),
        })
      case 'put':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) =>
            s3Protocol.putObject({
              Body: req as any,
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
            }),
          uploadId: (s3Protocol) =>
            s3Protocol.uploadPart({
              Body: req.raw,
              UploadId: req.query?.uploadId,
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
              PartNumber: req.query?.partNumber,
              ContentLength: req.headers?.['content-length'],
            }),
        })
      case 'head':
        return this.matchQueryString(req.query, {
          '*': (s3Protocol) =>
            s3Protocol.headObject({
              Bucket: this.getParam(req, 'Bucket'),
              Key: this.getParam(req, '*'),
            }),
        })
      // case 'delete':
      //   return this.matchQueryString(req.query, {
      //     '*': (s3Protocol) =>
      //       s3Protocol.deleteObject(this.getParam(req, 'Bucket'), this.getParam(req, 'Key')),
      //   })
    }
  }

  protected matchQueryString(querystring: unknown, objs: Record<any, Handler>) {
    if (!querystring) {
      if (objs['*']) {
        return objs['*']
      }
    }

    if (typeof querystring !== 'object') {
      throw new Error('invalid querystring format')
    }

    const q = querystring as Record<string, string>
    const matchingKeys = Object.keys(q).find((key) => {
      return objs[key]
    })

    if (!matchingKeys) {
      return objs['*']
    }

    return objs[matchingKeys]
  }

  protected getParam(req: EndpointSchema, param: string) {
    const value = (req.params as Record<string, string | undefined>)[param]
    if (!value) {
      throw new Error(`missing param: ${param}`)
    }
    return value
  }
}
