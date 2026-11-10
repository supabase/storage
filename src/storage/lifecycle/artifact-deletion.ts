import { lifecycleArtifactOutcomes } from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import type { Database } from '../database'
import type { Storage } from '../storage'

const MAX_S3_KEY_BYTES = 1024

export async function deleteLifecycleArtifacts({
  storage,
  database,
  artifacts,
  signal,
}: {
  storage: Storage
  database: Database
  artifacts: Array<{ bucketId: string; key: string }>
  signal: AbortSignal
}): Promise<{ absent: number; deleted: number; failed: number; error?: string }> {
  const isS3Backend = getConfig().storageBackendType === 's3'
  const absent: typeof artifacts = []
  const sendable: typeof artifacts = []
  const physicalKeys: string[] = []
  for (const artifact of artifacts) {
    const physicalKey = storage.location.getKeyLocation({
      tenantId: database.tenantId,
      bucketId: artifact.bucketId,
      objectName: artifact.key,
    })
    // A key beyond the S3 limit cannot exist and the backend rejects it on every attempt.
    if (isS3Backend && Buffer.byteLength(physicalKey) > MAX_S3_KEY_BYTES) {
      absent.push(artifact)
    } else {
      sendable.push(artifact)
      physicalKeys.push(physicalKey)
    }
  }
  const results =
    physicalKeys.length === 0
      ? []
      : await storage.backend.deleteObjectsDetailed(
          storage.location.getRootLocation(),
          physicalKeys,
          { signal }
        )
  const deleted: typeof artifacts = []
  const failed: typeof artifacts = []
  let error: string | undefined
  sendable.forEach((artifact, index) => {
    const result = results[index]
    if (result?.key === physicalKeys[index] && result.outcome === 'DELETED') {
      deleted.push(artifact)
      return
    }
    failed.push(artifact)
    error ??= result?.error?.message || result?.error?.code
  })
  await database.resolveLifecycleAbandonedArtifacts([...absent, ...deleted], failed)
  for (const [outcome, count] of [
    ['DELETED', deleted.length],
    ['ABSENT', absent.length],
    ['FAILED', failed.length],
  ] as const) {
    if (count) lifecycleArtifactOutcomes.add(count, { outcome })
  }
  return { absent: absent.length, deleted: deleted.length, failed: failed.length, error }
}
