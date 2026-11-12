import { ERRORS } from '@internal/errors'
import { logger } from '@internal/monitoring'
import type { StorageBackendAdapter } from './backend'
import { ObjectAdminDeleteBatch, type ObjectDeleteBatchEvent } from './events'

/** Call only after metadata commits. */
export async function deleteObjectContent(
  payload: ObjectDeleteBatchEvent,
  remove: () => Promise<void>
) {
  if (payload.objects.length === 0) return

  try {
    await remove()
  } catch (error) {
    const context = {
      project: payload.tenant.ref,
      type: 'storage',
      bucketId: payload.bucketId,
      reqId: payload.reqId,
      sbReqId: payload.sbReqId,
      objectCount: payload.objects.length,
    }
    logger.warn({ error, ...context }, 'Backend deletion failed after commit; scheduling cleanup')

    try {
      // A failed append degrades to running the handler inline (the queue's sync fallback).
      await ObjectAdminDeleteBatch.send(payload)
    } catch (error) {
      logger.error(
        { error, ...context, objects: payload.objects },
        'Failed to schedule deleted object cleanup'
      )
      throw error
    }
  }
}

export async function deleteBackendObjects(
  backend: StorageBackendAdapter,
  bucket: string,
  keys: string[]
) {
  const results = await backend.deleteObjectsDetailed(bucket, keys)
  const failed = results.filter(({ outcome }) => outcome !== 'DELETED')
  if (failed.length > 0) {
    throw ERRORS.InternalError(
      undefined,
      `Backend did not confirm deletion of ${failed.length} of ${keys.length} keys`
    ).withMetadata({ failure: failed[0] })
  }
}
