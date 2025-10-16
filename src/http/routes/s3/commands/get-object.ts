import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'
import { ERRORS } from '@internal/errors'

const GetObjectInput = {
  summary: 'Get Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
  Headers: {
    type: 'object',
    properties: {
      range: { type: 'string' },
      'if-none-match': { type: 'string' },
      'if-modified-since': { type: 'string' },
    },
  },
  Querystring: {
    type: 'object',
    properties: {
      'response-content-disposition': { type: 'string' },
      'response-content-type': { type: 'string' },
      'response-cache-control': { type: 'string' },
      'response-content-encoding': { type: 'string' },
      'response-content-language': { type: 'string' },
      'response-expires': { type: 'string' },
    },
  },
} as const

const GetObjectTagging = {
  summary: 'Get Object Tagging',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
  Querystring: {
    type: 'object',
    properties: {
      tagging: { type: 'string' },
    },
    required: ['tagging'],
  },
} as const

function parseDateHeader(input?: string) {
  if (input) {
    const parsedDate = new Date(input)
    if (isNaN(parsedDate.getTime())) {
      throw ERRORS.InvalidParameter('response-expires')
    }
    return parsedDate
  }
}

export default function GetObject(s3Router: S3Router) {
  s3Router.get(
    '/:Bucket/*?tagging',
    { schema: GetObjectTagging, operation: ROUTE_OPERATIONS.S3_GET_OBJECT_TAGGING },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.getObjectTagging({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
      })
    }
  )

  s3Router.get(
    '/:Bucket/*',
    { type: 'iceberg', schema: GetObjectInput, operation: ROUTE_OPERATIONS.S3_GET_OBJECT },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      const ifModifiedSince = req.Headers?.['if-modified-since']
      const icebergBucket = ctx.req.internalIcebergBucketName
      const responseExpires = parseDateHeader(req.Querystring?.['response-expires'])

      return s3Protocol.getObject(
        {
          Bucket: icebergBucket,
          Key: req.Params['*'],
          Range: req.Headers?.['range'],
          IfNoneMatch: req.Headers?.['if-none-match'],
          IfModifiedSince: ifModifiedSince ? new Date(ifModifiedSince) : undefined,
          ResponseContentDisposition: req.Querystring?.['response-content-disposition'],
          ResponseContentType: req.Querystring?.['response-content-type'],
          ResponseCacheControl: req.Querystring?.['response-cache-control'],
          ResponseContentEncoding: req.Querystring?.['response-content-encoding'],
          ResponseContentLanguage: req.Querystring?.['response-content-language'],
          ResponseExpires: responseExpires,
        },
        {
          skipDbCheck: true,
          signal: ctx.signals.response,
        }
      )
    }
  )

  s3Router.get(
    '/:Bucket/*',
    { schema: GetObjectInput, operation: ROUTE_OPERATIONS.S3_GET_OBJECT },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      const ifModifiedSince = req.Headers?.['if-modified-since']
      const responseExpires = parseDateHeader(req.Querystring?.['response-expires'])

      return s3Protocol.getObject(
        {
          Bucket: req.Params.Bucket,
          Key: req.Params['*'],
          Range: req.Headers?.['range'],
          IfNoneMatch: req.Headers?.['if-none-match'],
          IfModifiedSince: ifModifiedSince ? new Date(ifModifiedSince) : undefined,
          ResponseContentDisposition: req.Querystring?.['response-content-disposition'],
          ResponseContentType: req.Querystring?.['response-content-type'],
          ResponseCacheControl: req.Querystring?.['response-cache-control'],
          ResponseContentEncoding: req.Querystring?.['response-content-encoding'],
          ResponseContentLanguage: req.Querystring?.['response-content-language'],
          ResponseExpires: responseExpires,
        },
        {
          signal: ctx.signals.response,
        }
      )
    }
  )
}
