import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const GetBucketLifecycleInput = {
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

export default function GetBucketLifecycle(s3Router: S3Router) {
  s3Router.get(
    '/:Bucket?lifecycle',
    { schema: GetBucketLifecycleInput, operation: ROUTE_OPERATIONS.S3_GET_BUCKET_LIFECYCLE },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.getBucketLifecycle(req.Params.Bucket)
    }
  )

  s3Router.get(
    '/:Bucket?lifecycle',
    {
      type: 'iceberg',
      schema: GetBucketLifecycleInput,
      operation: ROUTE_OPERATIONS.S3_GET_BUCKET_LIFECYCLE,
    },
    async () => {
      throw ERRORS.LifecycleRequiresStandardBucket()
    }
  )
}
