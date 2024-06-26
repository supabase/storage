import { getConfig } from '../config'
import {
  getFileSizeLimit as getFileSizeLimitForTenant,
  getFeatures,
} from '../internal/database/tenant'
import { ERRORS } from '../internal/errors'

const { isMultitenant, imageTransformationEnabled } = getConfig()

/**
 * Get the maximum file size for a specific project
 * @param tenantId
 * @param maxUpperLimit
 */
export async function getFileSizeLimit(
  tenantId: string,
  maxUpperLimit?: number | null
): Promise<number> {
  let { uploadFileSizeLimit } = getConfig()
  if (isMultitenant) {
    uploadFileSizeLimit = await getFileSizeLimitForTenant(tenantId)
  }

  if (maxUpperLimit) {
    return Math.min(uploadFileSizeLimit, maxUpperLimit)
  }

  return uploadFileSizeLimit
}

/**
 * Determines if the image transformation feature is enabled.
 * @param tenantId
 */
export async function isImageTransformationEnabled(tenantId: string) {
  if (!isMultitenant) {
    return imageTransformationEnabled
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
 */
export function mustBeValidKey(key?: string): asserts key is string {
  if (!key || !isValidKey(key)) {
    throw ERRORS.InvalidKey(key || '')
  }
}

/**
 * Validates if a given bucket name is valid
 * throws if invalid
 * @param key
 */
export function mustBeValidBucketName(key?: string): asserts key is string {
  if (!key || !isValidBucketName(key)) {
    throw ERRORS.InvalidBucketName(key || '')
  }
}

export function parseFileSizeToBytes(valueWithUnit: string) {
  const valuesRegex = /(^[0-9]+(?:\.[0-9]+)?)(gb|mb|kb|b)$/i

  if (!valuesRegex.test(valueWithUnit)) {
    throw ERRORS.InvalidFileSizeLimit()
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
      throw ERRORS.InvalidFileSizeLimit()
  }
}

export function isUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

export function isEmptyFolder(object: string) {
  return object.endsWith('.emptyFolderPlaceholder')
}
