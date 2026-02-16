import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'
import { ERRORS } from '@internal/errors'
import { DuckLakeAvroGenerator, isDuckLakeVirtualPath } from '@storage/protocols/iceberg/catalog/ducklake-avro'
import { getConfig } from '../../../../config'

const HeadObjectInput = {
  summary: 'Head Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
} as const

export default function HeadObject(s3Router: S3Router) {
  s3Router.head(
    '/:Bucket/*',
    { type: 'iceberg', schema: HeadObjectInput, operation: ROUTE_OPERATIONS.S3_HEAD_OBJECT },
    async (req, ctx) => {
      const key = req.Params['*']
      const { ducklakeVirtualPrefix, icebergCatalogMode, ducklakeSchema, ducklakeDataBucket } =
        getConfig()

      if (
        icebergCatalogMode === 'ducklake' &&
        key &&
        isDuckLakeVirtualPath(key, ducklakeVirtualPrefix)
      ) {
        const generator = new DuckLakeAvroGenerator({
          db: ctx.req.db.pool.acquire(),
          ducklakeSchema,
          virtualPrefix: ducklakeVirtualPrefix,
          dataBucket: ducklakeDataBucket,
        })
        const buffer = await generator.generate(key)
        const crypto = await import('crypto')
        const etag = crypto.createHash('md5').update(buffer).digest('hex')
        return {
          statusCode: 200,
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': buffer.length.toString(),
            etag: `"${etag}"`,
          },
        }
      }

      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      const icebergBucket = ctx.req.internalIcebergBucketName

      if (!icebergBucket) {
        throw ERRORS.InvalidParameter('Iceberg bucket not found in request context')
      }

      return s3Protocol.headObject({
        Bucket: icebergBucket,
        Key: req.Params['*'],
      })
    }
  )

  s3Router.head(
    '/:Bucket/*',
    { schema: HeadObjectInput, operation: ROUTE_OPERATIONS.S3_HEAD_OBJECT },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.dbHeadObject({
        Bucket: req.Params.Bucket,
        Key: req.Params['*'],
      })
    }
  )
}
