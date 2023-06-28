import { getConfig } from '../config'
import { getFileSizeLimit as getFileSizeLimitForTenant, getFeatures } from '../database/tenant'
import { StorageBackendError } from './errors'

const { isMultitenant, enableImageTransformation } = getConfig()

/**
 * Get the maximum file size for a specific project
 * @param tenantId
 */
export async function getFileSizeLimit(tenantId: string): Promise<number> {
  let { fileSizeLimit } = getConfig()
  if (isMultitenant) {
    fileSizeLimit = await getFileSizeLimitForTenant(tenantId)
  }
  return fileSizeLimit
}

/**
 * Determines if the image transformation feature is enabled.
 * @param tenantId
 */
export async function isImageTransformationEnabled(tenantId: string) {
  if (!isMultitenant) {
    return enableImageTransformation
  }

  const { imageTransformation } = await getFeatures(tenantId)

  return imageTransformation.enabled
}

/**
 * Validates if a given object key or bucket key is valid
 * @param key
 */
export function isValidKey(key: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  return key.length > 0 && /^(\w|\/|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/.test(key)
}

/**
 * Validates if a given object key or bucket key is valid
 * @param bucketName
 */
export function isValidBucketName(bucketName: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  // excluding / for bucketName
  return (
    bucketName.length > 0 && /^(\w|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/.test(bucketName)
  )
}

/**
 * Validates if a given object key is valid
 * throws if invalid
 * @param key
 * @param message
 */
export function mustBeValidKey(key: string, message: string) {
  if (!isValidKey(key)) {
    throw new StorageBackendError('Invalid Input', 400, message)
  }
}

/**
 * Validates if a given bucket name is valid
 * throws if invalid
 * @param key
 * @param message
 */
export function mustBeValidBucketName(key: string, message: string) {
  if (!isValidBucketName(key)) {
    throw new StorageBackendError('Invalid Input', 400, message)
  }
}

export function parseFileSizeToBytes(valueWithUnit: string) {
  const valuesRegex = /(^[0-9]+(?:\.[0-9]+)?)(gb|mb|kb|b)$/i

  if (!valuesRegex.test(valueWithUnit)) {
    throw new StorageBackendError(
      'file_size_limit',
      422,
      'the requested file_size_limit uses an invalid format, use 20GB / 20MB / 30KB / 3B'
    )
  }

  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const [, valueS, unit] = valueWithUnit.match(valuesRegex)!
  const value = +parseFloat(valueS).toPrecision(3)

  switch (unit.toUpperCase()) {
    case 'GB':
      return value * 1e9
    case 'MB':
      return value * 1e6
    case 'KB':
      return value * 1000
    case 'B':
      return value
    default:
      throw new StorageBackendError(
        'file_size_limit',
        422,
        'the requested file_size_limit unit is not supported, use GB/MB/KB/B'
      )
  }
}

export function isUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}
