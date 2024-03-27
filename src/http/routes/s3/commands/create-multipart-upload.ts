import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const CreateMultiPartUploadInput = {
  summary: 'Create multipart upload',
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
      uploads: { type: 'string' },
    },
    required: ['uploads'],
  },
  Headers: {
    type: 'object',
    properties: {
      'content-type': { type: 'string' },
      'cache-control': { type: 'string' },
      'content-disposition': { type: 'string' },
      'content-encoding': { type: 'string' },
    },
  },
  Body: {},
} as const

export default function CreateMultipartUpload(s3Router: S3Router) {
  s3Router.post('/:Bucket/*?uploads', CreateMultiPartUploadInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)

    return s3Protocol.createMultiPartUpload({
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
      ContentType: req.Headers?.['content-type'],
      CacheControl: req.Headers?.['cache-control'],
      ContentDisposition: req.Headers?.['content-disposition'],
      ContentEncoding: req.Headers?.['content-encoding'],
    })
  })
}
