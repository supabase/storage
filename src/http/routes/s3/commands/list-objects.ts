import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const ListObjectsInput = {
  summary: 'List Objects',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
  Querystring: {
    type: 'object',
    properties: {
      'list-type': { type: 'string' },
      delimiter: { type: 'string' },
      'encoding-type': { type: 'string', enum: ['url'] },
      'max-keys': { type: 'number' },
      prefix: { type: 'string' },
      'continuation-token': { type: 'string' },
      'start-after': { type: 'string' },
    },
    required: ['list-type'],
  },
} as const

export default function ListObjects(s3Router: S3Router) {
  s3Router.get('/:Bucket?list-type=2', ListObjectsInput, async (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)

    return s3Protocol.listObjectsV2({
      Bucket: req.Params.Bucket,
      Prefix: req.Querystring?.prefix || '',
      ContinuationToken: req.Querystring?.['continuation-token'],
      StartAfter: req.Querystring?.['start-after'],
      EncodingType: req.Querystring?.['encoding-type'],
      MaxKeys: req.Querystring?.['max-keys'],
      Delimiter: req.Querystring?.delimiter,
    })
  })
}
