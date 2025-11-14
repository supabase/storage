import { BaseEvent } from '../base-event'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload } from '@internal/queue'
import { getConfig } from '../../../config'
import {
  getCatalogAuthStrategy,
  IcebergCatalogReconciler,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog'

const { isMultitenant, icebergCatalogUrl, icebergCatalogAuthType } = getConfig()

type DeleteEmptyNamespacesPayload = BasePayload

export class ReconcileIcebergCatalog extends BaseEvent<DeleteEmptyNamespacesPayload> {
  static queueName = 'reconcile-iceberg-catalog'

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'exactly_once',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: 'iceberg-reconcile-catalog',
      singletonHours: 12,
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
