import { S3ProtocolHandler } from '../../../../storage/protocols/s3/s3-handler'
import { S3Router } from '../router'

const DeleteObjectInput = {
  summary: 'Delete Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
  Querystring: {},
} as const

const DeleteObjectsInput = {
  summary: 'Delete Objects',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
  Body: {
    type: 'object',
    properties: {
      Delete: {
        type: 'object',
        properties: {
          Object: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                Key: { type: 'string' },
              },
              required: ['Key'],
            },
          },
        },
        required: ['Object'],
      },
    },
    required: ['Delete'],
  },
  Querystring: {
    type: 'object',
    properties: {
      delete: { type: 'string' },
    },
    required: ['delete'],
  },
} as const

export default function DeleteObject(s3Router: S3Router) {
  // Delete multiple objects
  s3Router.post('/:Bucket?delete', DeleteObjectsInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

    return s3Protocol.deleteObjects({
      Bucket: req.Params.Bucket,
      Delete: {
        Objects: req.Body.Delete.Object,
      },
    })
  })

  // Delete single object
  s3Router.delete('/:Bucket/*', DeleteObjectInput, (req, ctx) => {
    const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

    return s3Protocol.deleteObject({
      Bucket: req.Params.Bucket,
      Key: req.Params['*'],
    })
  })
}
