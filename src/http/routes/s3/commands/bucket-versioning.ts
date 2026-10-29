import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import type { S3Router } from '../router'

const BucketVersioningInput = {
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
      versioning: { type: 'string' },
    },
    required: ['versioning'],
  },
} as const

const PutBucketVersioningInput = {
  ...BucketVersioningInput,
  Body: {
    type: 'object',
    properties: {
      // The content is checked by the handler, which answers a bad
      // configuration with S3's MalformedXML or
      // IllegalVersioningConfigurationException rather than a schema error.
      // <VersioningConfiguration/> without attributes parses as a string.
      VersioningConfiguration: {
        anyOf: [{ type: 'object' }, { type: 'string' }],
      },
    },
    required: ['VersioningConfiguration'],
  },
} as const

async function rejectIcebergVersioning(): Promise<never> {
  throw ERRORS.VersioningRequiresStandardBucket()
}

export default function BucketVersioning(s3Router: S3Router) {
  const path = '/:Bucket?versioning'
  const getOptions = {
    schema: BucketVersioningInput,
    operation: ROUTE_OPERATIONS.S3_GET_BUCKET_VERSIONING,
  }
  const putOptions = {
    schema: PutBucketVersioningInput,
    operation: ROUTE_OPERATIONS.S3_PUT_BUCKET_VERSIONING,
  }

  s3Router.get(path, getOptions, async (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
    await ctx.storage.findBucket(req.Params.Bucket)

    return s3Protocol.getBucketVersioning(req.Params.Bucket)
  })
  s3Router.get(path, { ...getOptions, type: 'iceberg' }, rejectIcebergVersioning)

  s3Router.put(path, putOptions, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
    return s3Protocol.putBucketVersioning(req.Params.Bucket, req.Body.VersioningConfiguration)
  })
  s3Router.put(path, { ...putOptions, type: 'iceberg' }, rejectIcebergVersioning)
}
