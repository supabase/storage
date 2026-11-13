import { logger } from '@internal/monitoring'
import type { BasePayload, QueuedPayload } from '@internal/queue'
import type { BatchContext, Message, SubscribeOptions } from '@internal/queue/manager'
import { BatchFailure, QueueBatchHandler } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { MAX_OBJECTS_PER_DELETE_BATCH } from '../../limits'
import { getObjectDeletionKeys, TenantLocation } from '../../locator'
import { getOrCreateStorageBackend, storageEvent } from '../base'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { storageS3Bucket } = getConfig()

export class ObjectAdminDelete extends storageEvent<ObjectDeleteEvent>({
  type: 'ObjectAdminDelete',
}) {}

/**
 * Deletes the content of objects whose metadata is already gone. A batch handler: one pull
 * becomes one backend delete request, whatever tenants the objects belong to, since every
 * tenant's keys live in the same backend bucket.
 *
 * The batch is capped at {@link MAX_OBJECTS_PER_DELETE_BATCH}: a versioned object deletes two
 * keys (the object and its `.info` sidecar), and the backend takes 1000 keys per request.
 */
export class ObjectAdminDeleteHandler extends QueueBatchHandler(ObjectAdminDelete) {
  override readonly options: SubscribeOptions = {
    prefetch: MAX_OBJECTS_PER_DELETE_BATCH,
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 25_000, // under the queue's pgboss expireInSeconds: 30
  }

  async handleBatch(ctx: BatchContext<QueuedPayload<ObjectDeleteEvent>>): Promise<void> {
    const backend = getOrCreateStorageBackend()
    const location = new TenantLocation(storageS3Bucket)

    // Two messages can name the same object: a key maps to every message that needs it gone.
    const waitingOn = new Map<string, Message<QueuedPayload<ObjectDeleteEvent>>[]>()
    for (const message of ctx.messages) {
      const { data } = message
      const keys = getObjectDeletionKeys(location, {
        tenantId: data.tenant.ref,
        bucketId: data.bucketId,
        objects: [data],
      })
      for (const key of keys) {
        const messages = waitingOn.get(key)
        if (messages === undefined) waitingOn.set(key, [message])
        else messages.push(message)
      }
    }

    let results: Awaited<ReturnType<typeof backend.deleteObjectsDetailed>>
    try {
      results = await backend.deleteObjectsDetailed(
        location.getRootLocation(),
        [...waitingOn.keys()],
        { signal: ctx.signal }
      )
    } catch (error) {
      for (const message of ctx.messages) logFailure(message, error)
      throw error
    }

    // Only the messages whose keys the backend did not confirm go back for a retry.
    const failed = new Map<Message<QueuedPayload<ObjectDeleteEvent>>, unknown>()
    for (const result of results) {
      if (result.outcome === 'DELETED') continue
      for (const message of waitingOn.get(result.key) ?? []) {
        if (!failed.has(message)) failed.set(message, result)
      }
    }
    if (failed.size === 0) return

    for (const [message, failure] of failed) logFailure(message, failure)
    throw new BatchFailure(failed.keys())
  }
}

function logFailure(message: Message<QueuedPayload<ObjectDeleteEvent>>, error: unknown): void {
  const { id, data } = message
  const s3Key = `${data.tenant.ref}/${data.bucketId}/${data.name}`
  logger.error(
    {
      error,
      jobId: id,
      type: 'event',
      event: 'ObjectAdminDelete',
      payload: JSON.stringify(data),
      objectPath: s3Key,
      objectVersion: data.version,
      tenantId: data.tenant.ref,
      project: data.tenant.ref,
      reqId: data.reqId,
      sbReqId: data.sbReqId,
    },
    `[Admin]: ObjectAdminDelete ${s3Key} - FAILED`
  )
}
