import { S3ServiceException } from '@aws-sdk/client-s3'

export function isS3Error(error: unknown): error is S3ServiceException {
  return !!error && typeof error === 'object' && '$metadata' in error
}
