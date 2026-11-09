import { logger, logSchema } from '@internal/monitoring'
import type { Database } from '../database'
import type { Storage } from '../storage'
import { deleteLifecycleArtifacts } from './artifact-deletion'

const ABANDONED_ARTIFACT_PAGE_SIZE = 1000
const ABANDONED_ARTIFACT_MIN_SEND_MS = 10_000

export async function retryAbandonedLifecycleArtifacts({
  storage,
  database,
  attemptedBefore,
  budgetMs,
  signal,
}: {
  storage: Storage
  database: Database
  attemptedBefore: string
  budgetMs: number
  signal?: AbortSignal
}): Promise<void> {
  const deadline = Date.now() + budgetMs
  while (Date.now() < deadline && !signal?.aborted) {
    const page = await database.listLifecycleAbandonedArtifacts(
      attemptedBefore,
      ABANDONED_ARTIFACT_PAGE_SIZE
    )
    if (page.length === 0) return
    const artifacts = page.filter(({ live }) => !live)
    if (artifacts.length < page.length) {
      logSchema.warning(logger, '[Lifecycle] Dropped abandoned artifacts owned by live objects', {
        type: 'event',
        tenantId: database.tenantId,
        project: database.tenantId,
        metadata: JSON.stringify({ dropped: page.length - artifacts.length }),
      })
    }
    if (artifacts.length === 0) continue
    const { absent, deleted, failed, error } = await deleteLifecycleArtifacts({
      storage,
      database,
      artifacts,
      signal: AbortSignal.timeout(Math.max(ABANDONED_ARTIFACT_MIN_SEND_MS, deadline - Date.now())),
    })
    if (failed > 0) {
      logSchema.warning(logger, '[Lifecycle] Abandoned artifacts remain after retry', {
        type: 'event',
        tenantId: database.tenantId,
        project: database.tenantId,
        metadata: JSON.stringify({ failed, error }),
      })
    }
    if (absent + deleted === 0) return
  }
}
