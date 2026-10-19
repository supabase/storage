import { logger, logSchema } from '@internal/monitoring'

export interface LifecycleDeletedObjectRow {
  is_delete_marker: boolean
  metadata: Record<string, unknown> | null
}

export function lifecycleDeletionCounters(
  rows: LifecycleDeletedObjectRow[],
  context: { tenantId: string; bucketId: string }
): {
  objectVersions: number
  deleteMarkers: number
  bytes: number
} {
  let objectVersions = 0
  let deleteMarkers = 0
  let bytes = 0
  let invalidSizeCount = 0
  for (const row of rows) {
    if (row.is_delete_marker) {
      deleteMarkers++
      continue
    }
    objectVersions++
    const metadata = row.metadata
    const rawSize = metadata && typeof metadata === 'object' ? metadata.size : undefined
    const size =
      typeof rawSize === 'number' || (typeof rawSize === 'string' && rawSize.trim() !== '')
        ? Number(rawSize)
        : NaN
    if (!Number.isSafeInteger(size) || size < 0 || !Number.isSafeInteger(bytes + size)) {
      invalidSizeCount++
      continue
    }
    bytes += size
  }
  if (invalidSizeCount > 0) {
    logSchema.warning(logger, '[Lifecycle] Missing or invalid object sizes counted as zero bytes', {
      type: 'event',
      tenantId: context.tenantId,
      project: context.tenantId,
      metadata: JSON.stringify({ bucketId: context.bucketId, invalidSizeCount }),
    })
  }
  return { objectVersions, deleteMarkers, bytes }
}
