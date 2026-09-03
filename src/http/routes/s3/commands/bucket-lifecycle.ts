import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { type Context, type S3Router } from '../router'

const BucketLifecycleInput = {
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

const PutBucketLifecycleInput = {
  ...BucketLifecycleInput,
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

async function rejectIcebergLifecycle(): Promise<never> {
  throw ERRORS.LifecycleRequiresStandardBucket()
}

function registerLifecycleRoute(
  s3Router: S3Router,
  method: 'get' | 'put' | 'delete',
  schema: typeof BucketLifecycleInput | typeof PutBucketLifecycleInput,
  operation: string,
  handler: (
    req: { Params: { Bucket: string }; Body?: unknown },
    ctx: Context
  ) => Promise<{ statusCode?: number; responseBody?: unknown }>
) {
  const path = '/:Bucket?lifecycle'
  const options = { schema, operation }
  s3Router.registerRoute(method, path, options, handler as never)
  s3Router.registerRoute(method, path, { ...options, type: 'iceberg' }, rejectIcebergLifecycle)
}

export default function BucketLifecycle(s3Router: S3Router) {
  registerLifecycleRoute(
    s3Router,
    'get',
    BucketLifecycleInput,
    ROUTE_OPERATIONS.S3_GET_BUCKET_LIFECYCLE,
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.getBucketLifecycle(req.Params.Bucket)
    }
  )

  registerLifecycleRoute(
    s3Router,
    'put',
    PutBucketLifecycleInput,
    ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE,
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.putBucketLifecycle(req.Params.Bucket, req.Body)
    }
  )

  registerLifecycleRoute(
    s3Router,
    'delete',
    BucketLifecycleInput,
    ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE,
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.deleteBucketLifecycle(req.Params.Bucket)
    }
  )
}
