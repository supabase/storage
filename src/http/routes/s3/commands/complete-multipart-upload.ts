import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const CompletedMultipartUpload = {
  summary: 'Complete multipart upload',
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
  Headers: {
    type: 'object',
    properties: {
      authorization: { type: 'string' },
    },
    additionalProperties: true,
    required: ['authorization'],
  },
  Body: {
    nullable: true,
    type: 'object',
    properties: {
      CompleteMultipartUpload: {
        type: 'object',
        properties: {
          Part: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                PartNumber: { type: 'integer' },
                ETag: { type: 'string' },
              },
              required: ['PartNumber', 'ETag'],
            },
          },
        },
      },
    },
  },
} as const

export default function CompleteMultipartUpload(s3Router: S3Router) {
  s3Router.post(
    '/:Bucket/*?uploadId',
    { schema: CompletedMultipartUpload, operation: ROUTE_OPERATIONS.S3_COMPLETE_MULTIPART },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.completeMultiPartUpload({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
        UploadId: req.Querystring.uploadId,
        MultipartUpload: {
          Parts: req.Body?.CompleteMultipartUpload?.Part || [],
        },
      })
    }
  )
}
