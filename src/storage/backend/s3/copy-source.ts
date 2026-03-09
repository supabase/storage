import { encodeBucketAndObjectPath } from '../../path-encoding'

export function encodeCopySource(bucket: string, key: string): string {
  return encodeBucketAndObjectPath(bucket, key)
}
