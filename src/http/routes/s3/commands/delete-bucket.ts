import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const DeleteBucketInput = {
  summary: 'Delete Bucket',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
} as const

export default function DeleteBucket(s3Router: S3Router) {
  s3Router.delete(
    '/:Bucket',
    { schema: DeleteBucketInput, operation: ROUTE_OPERATIONS.S3_DELETE_BUCKET },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.deleteBucket(req.Params.Bucket)
    }
  )
}
