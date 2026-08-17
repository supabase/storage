import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const DeleteBucketLifecycleInput = {
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
      lifecycle: { type: 'string' },
    },
    required: ['lifecycle'],
  },
} as const

export default function DeleteBucketLifecycle(s3Router: S3Router) {
  s3Router.delete(
    '/:Bucket?lifecycle',
    { schema: DeleteBucketLifecycleInput, operation: ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.deleteBucketLifecycle(req.Params.Bucket)
    }
  )

  s3Router.delete(
    '/:Bucket?lifecycle',
    {
      type: 'iceberg',
      schema: DeleteBucketLifecycleInput,
      operation: ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE,
    },
    async () => {
      throw ERRORS.LifecycleRequiresStandardBucket()
    }
  )
}
