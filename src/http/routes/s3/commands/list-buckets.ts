import { MAX_BUCKETS, S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const ListBucketsInput = {
  summary: 'List buckets',
  Querystring: {
    type: 'object',
    properties: {
      prefix: { type: 'string', pattern: '^[^\u0000]*$' },
      'max-buckets': { type: 'integer', minimum: 1, maximum: MAX_BUCKETS, finite: true },
      'continuation-token': { type: 'string', maxLength: 1024 },
    },
  },
} as const

export default function ListBuckets(s3Router: S3Router) {
  s3Router.get(
    '/',
    { schema: ListBucketsInput, operation: ROUTE_OPERATIONS.S3_LIST_BUCKET },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.listBuckets({
        Prefix: req.Querystring?.prefix,
        MaxBuckets: req.Querystring?.['max-buckets'],
        ContinuationToken: req.Querystring?.['continuation-token'],
      })
    }
  )
}
