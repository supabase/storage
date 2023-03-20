import { mustBeValidBucketName, mustBeValidKey } from '../../../storage/limits'
import { StorageBackendError } from '../../../storage'

interface ResourceIDOptions {
  tenant: string
  bucket: string
  objectName: string
  version: string
}

export class UploadId {
  public tenant: string
  public readonly bucket: string
  public readonly objectName: string
  public readonly version: string

  constructor(options: ResourceIDOptions) {
    this.tenant = options.tenant
    this.bucket = options.bucket
    this.objectName = options.objectName
    this.version = options.version

    mustBeValidBucketName(options.bucket, 'invalid bucket name')
    mustBeValidKey(options.objectName, 'invalid object name')

    if (!options.tenant) {
      throw new StorageBackendError('tenant_not_found', 422, 'tenant not provided')
    }

    if (!options.version) {
      throw new StorageBackendError('version_not_found', 422, 'version not provided')
    }
  }

  static fromString(id: string) {
    const idParts = id.split('/')

    if (idParts.length < 4) {
      throw new StorageBackendError('id_missmatch', 422, 'id format invalid')
    }

    const [tenant, bucket, ...objParts] = idParts
    const version = objParts.pop()
    const objectName = objParts.join('/')

    return new this({
      version: version || '',
      objectName,
      bucket,
      tenant,
    })
  }

  toString() {
    return `${this.tenant}/${this.bucket}/${this.objectName}/${this.version}`
  }
}
