import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'

const ListObjectsInput = {
  summary: 'List buckets',
} as const

export default function ListBuckets(s3Router: S3Router) {
  s3Router.get(
    '/',
    { schema: ListObjectsInput, operation: ROUTE_OPERATIONS.S3_LIST_BUCKET },
    (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)
      return s3Protocol.listBuckets()
    }
  )
}
