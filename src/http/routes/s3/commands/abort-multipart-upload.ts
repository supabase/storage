import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const AbortMultiPartUploadInput = {
  summary: 'Abort MultiPart Upload',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
  Querystring: {
    type: 'object',
    properties: {
      uploadId: { type: 'string' },
    },
    required: ['uploadId'],
  },
} as const

export default function AbortMultiPartUpload(s3Router: S3Router) {
  s3Router.delete('/:Bucket/*?uploadId', AbortMultiPartUploadInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

    return s3Protocol.abortMultipartUpload({
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
      UploadId: req.Querystring.uploadId,
    })
  })
}
