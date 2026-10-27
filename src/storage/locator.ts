import { withOptionalVersion } from '@storage/backend'

export interface StorageObjectLocator {
  getRootLocation(): string
  getKeyLocation(params: {
    tenantId: string
    bucketId: string
    objectName: string
    version?: string
  }): string
}

export function getObjectDeletionKeys(
  location: StorageObjectLocator,
  params: {
    tenantId: string
    bucketId: string
    objects: readonly { name: string; version?: string | null }[]
  }
): string[] {
  return params.objects.flatMap(({ name, version }) => {
    const key = location.getKeyLocation({
      tenantId: params.tenantId,
      bucketId: params.bucketId,
      objectName: name,
      version: version ?? undefined,
    })
    // TUS sidecars belong to versioned keys; a legacy key.info can be another object.
    return version ? [key, `${key}.info`] : [key]
  })
}

export class TenantLocation implements StorageObjectLocator {
  constructor(protected readonly internalBucket: string) {}

  getRootLocation(): string {
    return this.internalBucket
  }

  getKeyLocation(params: {
    tenantId: string
    bucketId: string
    objectName: string
    version?: string
  }) {
    const key = `${params.tenantId}/${params.bucketId}/${withOptionalVersion(
      params.objectName,
      params.version
    )}`

    return key
  }
}

export class PassThroughLocation implements StorageObjectLocator {
  constructor(protected readonly internalBucket: string) {}

  getRootLocation(): string {
    return this.internalBucket
  }
  getKeyLocation(params: {
    tenantId: string
    bucketId: string
    objectName: string
    version?: string
  }) {
    return params.objectName
  }
}
