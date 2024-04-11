import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const ListObjectsInput = {
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

export default function ListObjects(s3Router: S3Router) {
  s3Router.get('/:Bucket/*', ListObjectsInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
    const ifModifiedSince = req.Headers?.['if-modified-since']

    return s3Protocol.getObject({
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
      Range: req.Headers?.['range'],
      IfNoneMatch: req.Headers?.['if-none-match'],
      IfModifiedSince: ifModifiedSince ? new Date(ifModifiedSince) : undefined,
    })
  })
}
