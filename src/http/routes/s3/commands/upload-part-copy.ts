import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const UploadPartCopyInput = {
  summary: 'Upload Part Copy',
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
      partNumber: { type: 'number', minimum: 1, maximum: 1000 },
    },
    required: ['uploadId', 'partNumber'],
  },
  Headers: {
    type: 'object',
    properties: {
      'x-amz-copy-source': { type: 'string' },
      'x-amz-copy-source-range': { type: 'string' },
      'x-amz-copy-source-if-match': { type: 'string' },
      'x-amz-copy-source-if-modified-since': { type: 'string' },
      'x-amz-copy-source-if-none-match': { type: 'string' },
      'x-amz-copy-source-if-unmodified-since': { type: 'string' },
      expires: { type: 'string' },
    },
    required: ['x-amz-copy-source'],
  },
} as const

export default function UploadPartCopy(s3Router: S3Router) {
  s3Router.put(
    '/:Bucket/*?partNumber&uploadId|x-amz-copy-source',
    { schema: UploadPartCopyInput, operation: ROUTE_OPERATIONS.S3_UPLOAD_PART_COPY },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.uploadPartCopy({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
        CopySource: req.Headers['x-amz-copy-source'],
        PartNumber: req.Querystring.partNumber,
        UploadId: req.Querystring.uploadId,
        CopySourceRange: req.Headers['x-amz-copy-source-range'],
        CopySourceIfMatch: req.Headers['x-amz-copy-source-if-match'],
        CopySourceIfModifiedSince: req.Headers['x-amz-copy-source-if-modified-since']
          ? new Date(req.Headers['x-amz-copy-source-if-modified-since'])
          : undefined,
        CopySourceIfNoneMatch: req.Headers['x-amz-copy-source-if-none-match'],
        CopySourceIfUnmodifiedSince: req.Headers['x-amz-copy-source-if-unmodified-since']
          ? new Date(req.Headers['x-amz-copy-source-if-unmodified-since'])
          : undefined,
      })
    }
  )
}
