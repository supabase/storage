import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import {
  getCatalogAuthStrategy,
  IcebergCatalogReconciler,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'
import { getConfig } from '../../../config'
import { DEDUP_TTL_12H, storageEvent } from '../base'

const { isMultitenant, icebergCatalogUrl, icebergCatalogAuthType, pgQueueConcurrentTasksPerQueue } =
  getConfig()

export type ReconcileIcebergCatalogPayload = BasePayload

/** Registered worker with no producer (v1 status quo) — ready for a future scheduled send. */
export class ReconcileIcebergCatalog extends storageEvent<ReconcileIcebergCatalogPayload>({
  type: 'ReconcileIcebergCatalog',
  idempotencyKey: () => 'iceberg-reconcile-catalog',
  idempotencyTtlMs: DEDUP_TTL_12H,
}) {}

export class ReconcileIcebergCatalogHandler extends QueueHandler(ReconcileIcebergCatalog) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
  }

  async handle(_ctx: StorageJob<ReconcileIcebergCatalogPayload>): Promise<void> {
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
