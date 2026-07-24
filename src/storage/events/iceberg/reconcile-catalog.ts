import { BasePayload } from '@internal/queue'
import {
  getCatalogAuthStrategy,
  IcebergCatalogReconciler,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { BaseEvent } from '../base-event'

const { isMultitenant, icebergCatalogUrl, icebergCatalogAuthType } = getConfig()

type DeleteEmptyNamespacesPayload = BasePayload

export class ReconcileIcebergCatalog extends BaseEvent<DeleteEmptyNamespacesPayload> {
  static queueName = 'reconcile-iceberg-catalog'

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'exclusive',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(): SendOptions {
    return {
      expireInSeconds: 2 * 60 * 60,
      singletonKey: 'iceberg-reconcile-catalog',
      singletonSeconds: 12 * 60 * 60,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: Job<DeleteEmptyNamespacesPayload>) {
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
