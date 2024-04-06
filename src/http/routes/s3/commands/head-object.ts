import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const HeadObjectInput = {
  summary: 'Head Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
} as const

export default function HeadObject(s3Router: S3Router) {
  s3Router.head('/:Bucket/*', HeadObjectInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

    return s3Protocol.headObject({
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
    })
  })
}
