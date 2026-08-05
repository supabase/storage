import type { Bucket, IcebergCatalog, Obj, S3MultipartUpload } from '../schemas'
import { defineColumnSet, staticSqlLiteral } from './column-set'

export type { ColumnSelection } from './column-set'

// type-only set brands
declare const objectColumnSetId: unique symbol
declare const bucketColumnSetId: unique symbol
declare const multipartColumnSetId: unique symbol
declare const analyticsColumnSetId: unique symbol

export const objectColumns = defineColumnSet<Obj, typeof objectColumnSetId>({
  fallback: 'id',
  availableFrom: {
    user_metadata: 'custom-metadata',
  },
})

export const bucketColumns = defineColumnSet<Bucket, typeof bucketColumnSetId>({
  fallback: 'id',
  availableFrom: {
    type: 'iceberg-catalog-flag-on-buckets',
  },
  synthetic: {
    type: staticSqlLiteral('STANDARD'),
  },
})

export const multipartColumns = defineColumnSet<S3MultipartUpload, typeof multipartColumnSetId>({
  fallback: 'id',
  availableFrom: {
    user_metadata: 'custom-metadata',
    metadata: 's3-multipart-uploads-metadata',
  },
})

export const analyticsColumns = defineColumnSet<IcebergCatalog, typeof analyticsColumnSetId>({
  fallback: 'id',
})

export type ObjectColumnSelection = ReturnType<typeof objectColumns.select>
export type BucketColumnSelection = ReturnType<typeof bucketColumns.select>
export type MultipartColumnSelection = ReturnType<typeof multipartColumns.select>
export type AnalyticsColumnSelection = ReturnType<typeof analyticsColumns.select>

export const OBJECT_ID_COLUMNS = objectColumns.select('id')
export const OBJECT_ALL_COLUMNS = objectColumns.select('*')
export const BUCKET_ID_COLUMNS = bucketColumns.select('id')
export const MULTIPART_ID_COLUMNS = multipartColumns.select('id')
export const ANALYTICS_NAME_COLUMNS = analyticsColumns.select('name')
