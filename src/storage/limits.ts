import { getConfig } from '../config'
import { getFileSizeLimit as getFileSizeLimitForTenant } from '../database/tenant'
import { StorageBackendError } from './errors'

const { isMultitenant } = getConfig()

export async function getFileSizeLimit(tenantId: string): Promise<number> {
  let { fileSizeLimit } = getConfig()
  if (isMultitenant) {
    fileSizeLimit = await getFileSizeLimitForTenant(tenantId)
  }
  return fileSizeLimit
}

export function isValidKey(key: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  return key.length > 0 && /^(\w|\/|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/.test(key)
}

export function mustBeValidKey(key: string, message: string) {
  if (!isValidKey(key)) {
    throw new StorageBackendError('Invalid Input', 400, message)
  }
}
