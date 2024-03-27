import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const GetBucketLocationInput = {
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
      location: { type: 'string' },
    },
    required: ['location'],
  },
} as const

const GetBucketVersioningInput = {
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

export default function GetBucket(s3Router: S3Router) {
  s3Router.get('/:Bucket?location', GetBucketLocationInput, async (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)
    await ctx.storage.findBucket(req.Params.Bucket)

    return s3Protocol.getBucketLocation()
  })

  s3Router.get('/:Bucket?versioning', GetBucketVersioningInput, async (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)
    await ctx.storage.findBucket(req.Params.Bucket)

    return s3Protocol.getBucketVersioning()
  })
}
