import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const ListPartsInput = {
  summary: 'List Parts',
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
      uploadId: { type: 'string' },
      'max-parts': { type: 'number', minimum: 1, maximum: 1000 },
      'part-number-marker': { type: 'string' },
    },
    required: ['uploadId'],
  },
} as const

export default function ListParts(s3Router: S3Router) {
  s3Router.get(
    '/:Bucket/*?uploadId',
    { schema: ListPartsInput, operation: ROUTE_OPERATIONS.S3_LIST_PARTS },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.listParts({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
        UploadId: req.Querystring.uploadId,
        MaxParts: req.Querystring['max-parts'],
        PartNumberMarker: req.Querystring['part-number-marker'],
      })
    }
  )
}
