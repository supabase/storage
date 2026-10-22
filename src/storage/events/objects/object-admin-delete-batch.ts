import type { BasePayload } from '@internal/queue'
import type { Job, SendOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { getObjectDeletionKeys, TenantLocation } from '../../locator'
import { deleteBackendObjects } from '../../object-cleanup'
import { BaseEvent } from '../base-event'

const { storageS3Bucket } = getConfig()

export interface ObjectDeleteBatchEvent extends BasePayload {
  bucketId: string
  objects: { name: string; version?: string | null }[]
}

export class ObjectAdminDeleteBatch extends BaseEvent<ObjectDeleteBatchEvent> {
  // Keep batch payloads away from workers expecting a single object.
  static queueName = 'object:admin:delete-batch'

  static getSendOptions(): SendOptions {
    return { priority: 10, expireInSeconds: 30 }
  }

  static async shouldSend() {
    return true
  }

  static async handle(job: Job<ObjectDeleteBatchEvent>) {
    const backend = this.getOrCreateStorageBackend()
    const location = new TenantLocation(storageS3Bucket)
    const { tenant, bucketId, objects } = job.data
    const keys = getObjectDeletionKeys(location, { tenantId: tenant.ref, bucketId, objects })
    await deleteBackendObjects(backend, location.getRootLocation(), keys)
  }
}
