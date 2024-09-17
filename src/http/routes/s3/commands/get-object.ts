import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

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
  Querystring: {},
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
    { schema: GetObjectInput, operation: ROUTE_OPERATIONS.S3_GET_OBJECT },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      const ifModifiedSince = req.Headers?.['if-modified-since']

      return s3Protocol.getObject(
        {
          Bucket: req.Params.Bucket,
          Key: req.Params['*'],
          Range: req.Headers?.['range'],
          IfNoneMatch: req.Headers?.['if-none-match'],
          IfModifiedSince: ifModifiedSince ? new Date(ifModifiedSince) : undefined,
        },
        {
          signal: ctx.signals.response,
        }
      )
    }
  )
}
