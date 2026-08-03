import type { BasePayload, WirePayload } from '@internal/queue'
import {
  getCatalogAuthStrategy,
  IcebergCatalogReconciler,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { DEDUP_TTL_12H, storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { isMultitenant, icebergCatalogUrl, icebergCatalogAuthType, pgQueueConcurrentTasksPerQueue } =
  getConfig()

export type ReconcileIcebergCatalogPayload = BasePayload

/** Registered worker with no producer (v1 status quo) — ready for a future scheduled send. */
export class ReconcileIcebergCatalog extends storageEvent<ReconcileIcebergCatalogPayload>({
  type: 'ReconcileIcebergCatalog',
  idempotencyKey: () => 'iceberg-reconcile-catalog',
  idempotencyTtlMs: DEDUP_TTL_12H,
}) {}

export class ReconcileIcebergCatalogHandler extends TopicHandler(ReconcileIcebergCatalog) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: systemRetry(TOPICS.reconcileIcebergCatalog),
  }

  async handle(_ctx: JobContext<WirePayload<ReconcileIcebergCatalogPayload>>): Promise<void> {
    if (!isMultitenant) {
      return
    }
    const restCatalog = new RestCatalogClient({
      catalogUrl: icebergCatalogUrl,
      auth: getCatalogAuthStrategy(icebergCatalogAuthType),
    })

    const reconciler = new IcebergCatalogReconciler(restCatalog)
    await reconciler.reconcile()
  }
}
