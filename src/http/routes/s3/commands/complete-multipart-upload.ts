import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'
import { ERRORS } from '@internal/errors'

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
    {
      type: 'iceberg',
      schema: CompletedMultipartUpload,
      operation: ROUTE_OPERATIONS.S3_COMPLETE_MULTIPART,
    },
    async (req, ctx) => {
      const icebergBucketName = ctx.req.internalIcebergBucketName

      if (!icebergBucketName) {
        throw ERRORS.InvalidParameter('internalIcebergBucketName')
      }

      const resp = await ctx.req.storage.backend.completeMultipartUpload(
        icebergBucketName,
        req.Params['*'],
        req.Querystring.uploadId,
        '',
        req.Body?.CompleteMultipartUpload?.Part || []
      )

      return {
        responseBody: {
          CompleteMultipartUploadResult: {
            Location: `${req.Params.Bucket}/${req.Params['*']}`,
            Bucket: req.Params.Bucket,
            Key: req.Params['*'],
            ChecksumCRC32: resp.ChecksumCRC32,
            ChecksumCRC32C: resp.ChecksumCRC32,
            ChecksumSHA1: resp.ChecksumSHA1,
            ChecksumSHA256: resp.ChecksumSHA256,
            ETag: resp.ETag,
          },
        },
      }
    }
  )

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
