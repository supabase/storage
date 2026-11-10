import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import type { S3Router } from '../router'
import { assertGuardedHeadersSigned } from './signed-header-guard'

const input = {
  summary: 'Get Object Attributes',
  Params: {
    type: 'object',
    properties: { Bucket: { type: 'string' }, '*': { type: 'string' } },
    required: ['Bucket', '*'],
  },
  Querystring: {
    type: 'object',
    properties: { attributes: { type: 'string' }, versionId: { type: 'string' } },
    required: ['attributes'],
  },
  Headers: {
    type: 'object',
    properties: {
      'x-amz-object-attributes': { type: 'string' },
    },
  },
} as const

export default function GetObjectAttributes(router: S3Router) {
  for (const type of [undefined, 'iceberg'] as const) {
    router.get(
      '/:Bucket/*?attributes',
      { type, schema: input, operation: ROUTE_OPERATIONS.S3_GET_OBJECT_ATTRIBUTES },
      (req, ctx) => {
        assertGuardedHeadersSigned(
          req.Headers,
          ctx.req.signedHeaders,
          (name) => name === 'x-amz-object-attributes'
        )
        const bucket = type === 'iceberg' ? ctx.req.internalIcebergBucketName : req.Params.Bucket
        if (!bucket) throw ERRORS.InvalidBucketName('Iceberg bucket name is required')
        const attributes = req.Headers?.['x-amz-object-attributes']
        return new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner).getObjectAttributes(
          {
            Bucket: bucket,
            Key: req.Params['*'],
            VersionId: req.Querystring.versionId,
            ObjectAttributes: attributes?.split(',').map((value) => value.trim()),
          },
          { iceberg: type === 'iceberg' }
        )
      }
    )
  }
}
