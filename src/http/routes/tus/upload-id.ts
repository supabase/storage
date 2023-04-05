import { mustBeValidBucketName, mustBeValidKey } from '../../../storage/limits'
import { StorageBackendError } from '../../../storage'
import { getConfig } from '../../../config'
import { FILE_VERSION_SEPARATOR, PATH_SEPARATOR, SEPARATOR } from '../../../storage/backend'

interface ResourceIDOptions {
  tenant: string
  bucket: string
  objectName: string
  version: string
}

const { tusUseFileVersionSeparator } = getConfig()

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
    const uploadInfo = tusUseFileVersionSeparator ? fromFileSeparator(id) : fromPathSeparator(id)

    return new this(uploadInfo)
  }

  toString() {
    const separator = tusUseFileVersionSeparator ? FILE_VERSION_SEPARATOR : PATH_SEPARATOR
    return `${this.tenant}/${this.bucket}/${this.objectName}${separator}${this.version}`
  }
}

function fromPathSeparator(id: string) {
  const idParts = id.split(PATH_SEPARATOR)

  if (idParts.length < 3) {
    throw new StorageBackendError('id_missmatch', 422, 'id format invalid')
  }

  const [tenant, bucket, ...objParts] = idParts
  const version = objParts.pop()

  if (!version) {
    throw new StorageBackendError('version_not_found', 422, 'version not provided')
  }

  return {
    version,
    objectName: objParts.join('/'),
    tenant,
    bucket,
  }
}

function fromFileSeparator(id: string) {
  const idParts = id.split(PATH_SEPARATOR)

  if (idParts.length < 3) {
    throw new StorageBackendError('id_missmatch', 422, 'id format invalid')
  }

  const [tenant, bucket, ...objParts] = idParts
  const objectWithVersion = objParts.pop()

  const separator = SEPARATOR
  const objectNameParts = objectWithVersion?.split(separator) || []

  if (objectNameParts.length < 2) {
    throw new StorageBackendError('object_name_invalid', 422, 'object name invalid')
  }

  const version = objectNameParts[1]
  const objectName = objectNameParts[0]

  if (!version) {
    throw new StorageBackendError('version_not_found', 422, 'version not provided')
  }

  objParts.push(objectName)

  return {
    version,
    objectName,
    tenant,
    bucket,
  }
}
