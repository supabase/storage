import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const PutObjectInput = {
  summary: 'Put Object',
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
  },
  Headers: {
    type: 'object',
    properties: {
      authorization: { type: 'string' },
      host: { type: 'string' },
      'x-amz-content-sha256': { type: 'string' },
      'x-amz-date': { type: 'string' },
      'content-type': { type: 'string' },
      'content-length': { type: 'integer' },
      'cache-control': { type: 'string' },
      'content-disposition': { type: 'string' },
      'content-encoding': { type: 'string' },
      expires: { type: 'string' },
    },
  },
} as const

const UploadPartInput = {
  summary: 'Upload Part',
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
      partNumber: { type: 'number', minimum: 1, maximum: 5000 },
    },
    required: ['uploadId', 'partNumber'],
  },
  Headers: {
    type: 'object',
    properties: {
      host: { type: 'string' },
      'x-amz-content-sha256': { type: 'string' },
      'x-amz-date': { type: 'string' },
      'content-type': { type: 'string' },
      'content-length': { type: 'integer' },
    },
    required: ['content-length'],
  },
} as const

export default function UploadPart(s3Router: S3Router) {
  s3Router.put(
    '/:Bucket/*?uploadId&partNumber',
    {
      schema: UploadPartInput,
      operation: ROUTE_OPERATIONS.S3_UPLOAD_PART,
      disableContentTypeParser: true,
    },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.uploadPart({
        Body: ctx.req.raw,
        UploadId: req.Querystring?.uploadId,
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
        PartNumber: req.Querystring?.partNumber,
        ContentLength: req.Headers?.['content-length'],
      })
    }
  )

  s3Router.put(
    '/:Bucket/*',
    {
      schema: PutObjectInput,
      operation: ROUTE_OPERATIONS.S3_UPLOAD,
      disableContentTypeParser: true,
    },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      const metadata = s3Protocol.parseMetadataHeaders(req.Headers)

      return s3Protocol.putObject(
        {
          Body: ctx.req as any,
          Bucket: req.Params.Bucket,
          Key: req.Params['*'],
          CacheControl: req.Headers?.['cache-control'],
          ContentType: req.Headers?.['content-type'],
          Expires: req.Headers?.['expires'] ? new Date(req.Headers?.['expires']) : undefined,
          ContentEncoding: req.Headers?.['content-encoding'],
          Metadata: metadata,
        },
        ctx.signals.body
      )
    }
  )
}
