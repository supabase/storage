import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

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
    additionalProperties: true,
    properties: {
      authorization: { type: 'string' },
      'content-type': { type: 'string' },
      'cache-control': { type: 'string' },
      'content-disposition': { type: 'string' },
      'content-encoding': { type: 'string' },
    },
    required: ['authorization'],
  },
} as const

export default function CreateMultipartUpload(s3Router: S3Router) {
  s3Router.post(
    '/:Bucket/*?uploads',
    { schema: CreateMultiPartUploadInput, operation: ROUTE_OPERATIONS.S3_CREATE_MULTIPART },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      const metadata = s3Protocol.parseMetadataHeaders(req.Headers)

      return s3Protocol.createMultiPartUpload({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
        ContentType: req.Headers?.['content-type'],
        CacheControl: req.Headers?.['cache-control'],
        ContentDisposition: req.Headers?.['content-disposition'],
        ContentEncoding: req.Headers?.['content-encoding'],
        Metadata: metadata,
      })
    }
  )
}
