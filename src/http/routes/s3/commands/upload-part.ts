import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

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
  s3Router.put('/:Bucket/*?uploadId&partNumber', UploadPartInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)

    return s3Protocol.uploadPart({
      Body: (req.raw as any).raw,
      UploadId: req.Querystring?.uploadId,
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
      PartNumber: req.Querystring?.partNumber,
      ContentLength: req.Headers?.['content-length'],
    })
  })

  s3Router.put('/:Bucket/*', PutObjectInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId)
    return s3Protocol.putObject({
      Body: req.raw,
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
    })
  })
}
