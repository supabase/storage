import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { createAgent } from '@internal/http'
import type { BasePayload, QueueEventOptions, WirePayload } from '@internal/queue'
import { TenantLocation } from '@storage/locator'
import type { MessageInit, MessageInstance } from '@supabase-labs/wave-core'
import { defineMessage } from '@supabase-labs/wave-core'
import { getConfig } from '../../config'
import { createStorageBackend, StorageBackendAdapter } from '../backend'
import type { Database } from '../database'
import { StoragePgDB } from '../database'
import { Storage } from '../storage'

const { storageS3Bucket, storageS3MaxSockets, storageBackendType, region } = getConfig()

/** A storage event message class: `new (payload)` stamps `region` and `$version` into the
 * wire data (v1's send-time injection) and carries the v1 eventName as its message type. Also
 * satisfies `QueueEventOptions` — `allowSync`/`disableKeys` are the queue middlewares' per-event
 * config, declared here instead of a separate metadata object. */
export interface StorageEventClass<T extends BasePayload> extends QueueEventOptions {
  new (payload: T, init?: MessageInit): MessageInstance<WirePayload<T>>
  readonly version: string
  disableKeys?(data: WirePayload<T>): string[]
}

/** v1 singletonHours windows, as per-message dedup TTLs (wave `idempotencyTtlMs` →
 * pg-boss `singletonSeconds`). Declared here (not in `topics.ts`) because event classes read
 * them at declaration time — this module is the one they already import eagerly. */
export const DEDUP_TTL_1H = 3_600_000
export const DEDUP_TTL_12H = 43_200_000

/**
 * Declare a storage event message (the shape of a v1 `Event` subclass's send side).
 * `idempotencyKey` is the v1 `singletonKey` derivation, now declared once on the class.
 */
export function storageEvent<T extends BasePayload>(opts: {
  type: string
  version?: string
  idempotencyKey?: (data: WirePayload<T>) => string
  /** The engine-side dedup window `idempotencyKey` applies over (v1 `singletonHours`).
   * Without it, `exclusive` topics still get the queued/active mutex — just no time window. */
  idempotencyTtlMs?: number
  /** Whether this event may run synchronously in-process when the queue degrades to inline
   * execution (v1 `allowSync`). Defaults to `true`; events whose failure must never silently
   * run inline (durable migrations, iceberg resource cleanup) override to `false`. */
  allowSync?: boolean
  /** All per-tenant disable keys this concrete message matches, derived from its payload.
   * Defaults to `[type]` (v1 `shouldSend`'s default); the webhook event overrides this to add
   * `Webhook:{type}` and `Webhook:{type}:{bucket}/{name}` scoping. */
  disableKeys?: (data: WirePayload<T>) => string[]
}): StorageEventClass<T> {
  const version = opts.version ?? 'v1'
  class StorageEventMessage extends defineMessage<WirePayload<T>>({
    ...(opts.idempotencyKey !== undefined ? { idempotencyKey: opts.idempotencyKey } : {}),
    ...(opts.idempotencyTtlMs !== undefined ? { idempotencyTtlMs: opts.idempotencyTtlMs } : {}),
  }) {
    static readonly eventType = opts.type
    static readonly version = version
    static readonly allowSync = opts.allowSync ?? true
    static readonly disableKeys = opts.disableKeys
    override readonly type = opts.type

    constructor(payload: T, init?: MessageInit) {
      super({ region, ...payload, $version: payload.$version ?? version } as WirePayload<T>, init)
    }
  }
  return StorageEventMessage as unknown as StorageEventClass<T>
}

let storageBackend: StorageBackendAdapter | undefined = undefined

/** The worker-shared storage backend (v1 `BaseEvent.getOrCreateStorageBackend`). */
export function getOrCreateStorageBackend(monitor = false): StorageBackendAdapter {
  if (storageBackend) {
    return storageBackend
  }

  const httpAgent = createAgent('s3_worker', {
    maxSockets: storageS3MaxSockets,
  })

  storageBackend = createStorageBackend(storageBackendType, {
    httpAgent,
  })

  if (monitor) {
    httpAgent.monitor()
  }

  return storageBackend
}

/** Teardown for the shared backend — wired into the queue's `onStop` (v1 `onClose`). */
export async function closeStorageBackend(): Promise<void> {
  storageBackend?.close()
  storageBackend = undefined
}

/** A service-key admin `Storage` bound to the payload's tenant (v1 `BaseEvent.createStorage`).
 * Callers own the connection: `storage.db.destroyConnection()` in a finally. */
export async function createStorage(payload: BasePayload): Promise<Storage> {
  const adminUser = await getServiceKeyUser(payload.tenant.ref)
  const connectionOptions = {
    user: adminUser,
    superUser: adminUser,
    host: payload.tenant.host,
    tenantId: payload.tenant.ref,
    disableHostCheck: true,
  }

  const databaseOptions = {
    tenantId: payload.tenant.ref,
    host: payload.tenant.host,
    reqId: payload.reqId,
    sbReqId: payload.sbReqId,
  }

  const db: Database = new StoragePgDB(
    await getPostgresConnection(connectionOptions),
    databaseOptions
  )

  return new Storage(getOrCreateStorageBackend(), db, new TenantLocation(storageS3Bucket))
}
