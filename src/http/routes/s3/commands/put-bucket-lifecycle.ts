import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const PutBucketLifecycleInput = {
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
  Body: {
    type: 'object',
    properties: {
      LifecycleConfiguration: {
        type: 'object',
        properties: {
          Rule: {
            type: 'array',
            items: { type: 'object', additionalProperties: true },
          },
        },
        required: ['Rule'],
        additionalProperties: true,
      },
    },
    required: ['LifecycleConfiguration'],
    additionalProperties: true,
  },
} as const

export default function PutBucketLifecycle(s3Router: S3Router) {
  s3Router.put(
    '/:Bucket?lifecycle',
    { schema: PutBucketLifecycleInput, operation: ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.putBucketLifecycle(req.Params.Bucket, req.Body)
    }
  )

  s3Router.put(
    '/:Bucket?lifecycle',
    {
      type: 'iceberg',
      schema: PutBucketLifecycleInput,
      operation: ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE,
    },
    async () => {
      throw ERRORS.LifecycleRequiresStandardBucket()
    }
  )
}
