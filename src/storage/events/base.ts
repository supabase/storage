import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { createAgent } from '@internal/http'
import type { BasePayload, QueuedPayload, QueueEventOptions } from '@internal/queue'
import { getQueue } from '@internal/queue'
import type { PgBossCtx } from '@internal/queue/adapters/pgboss'
import { SYSTEM_TENANT_REF } from '@internal/queue/constants'
import type {
  MessageInit,
  MessageInstance,
  ProduceBatchOptions,
  QueueClasses,
} from '@internal/queue/manager'
import { defineMessage } from '@internal/queue/manager'
import { TenantLocation } from '@storage/locator'
import { getConfig } from '../../config'
import { createStorageBackend, StorageBackendAdapter } from '../backend'
import type { Database } from '../database'
import { StoragePgDB } from '../database'
import { Storage } from '../storage'
import type { StorageQueues } from './registry'

const { storageS3Bucket, storageS3MaxSockets, storageBackendType, region } = getConfig()

/** A storage event message class: `new (payload)` stamps `region` and `$version` into the
 * wire data (v1's send-time injection) and carries the v1 eventName as its message type. Also
 * satisfies `QueueEventOptions` — `allowSync`/`disableKeys` are the queue middlewares' per-event
 * config, declared here instead of a separate metadata object. */
export interface StorageEventClass<T extends BasePayload> extends QueueEventOptions {
  new (payload: T, init?: MessageInit): MessageInstance<QueuedPayload<T>>
  readonly version: string
  disableKeys?(data: QueuedPayload<T>): string[]

  /**
   * Enqueue one message of this event on the storage queue: `X.send(payload)` is
   * `getStorageQueue().produce(new X(payload))`. The `this` constraint keeps the registry
   * check that `produce` makes: calling it on a class no queue in `storageQueues` binds is a
   * compile error, not a runtime "unbound message class" throw.
   */
  send(this: BoundStorageEvent, payload: T, options?: StorageSendOptions): Promise<void>
  /** Enqueue many messages of this event in one round-trip (atomic on transactional backends). */
  batchSend(
    this: BoundStorageEvent,
    payloads: ReadonlyArray<T>,
    options?: StorageSendOptions
  ): Promise<void>
  /** Run this event's handler in-process, without touching the queue. */
  invoke(this: BoundStorageEvent, payload: T): Promise<void>
}

/** A message class some queue in `storageQueues` binds: the only classes the queue accepts. */
export type BoundStorageEvent = QueueClasses<StorageQueues>

type BoundStorageMessage = InstanceType<BoundStorageEvent>

/** Per-call produce options: `ctx` carries a pg-boss `Db` for transactional enqueue (honoured
 * when pg-boss is the running engine). */
export type StorageSendOptions = ProduceBatchOptions<PgBossCtx>

/** v1 singletonHours windows, as per-message dedup TTLs (`idempotencyTtlMs`, pg-boss
 * `singletonSeconds`). Declared here (not in `registry.ts`) because event classes read them at
 * declaration time, and this module is the one they already import eagerly. */
export const DEDUP_TTL_1H = 3_600_000
export const DEDUP_TTL_12H = 43_200_000

/**
 * Declare a storage event message (the shape of a v1 `Event` subclass's send side).
 * `idempotencyKey` is the v1 `singletonKey` derivation, now declared once on the class.
 */
export function storageEvent<T extends BasePayload>(opts: {
  type: string
  version?: string
  idempotencyKey?: (data: QueuedPayload<T>) => string
  /** The engine-side dedup window `idempotencyKey` applies over (v1 `singletonHours`).
   * Without it, `exclusive` queues still get the queued/active mutex, just no time window. */
  idempotencyTtlMs?: number
  /** Whether this event may run synchronously in-process when the queue degrades to inline
   * execution (v1 `allowSync`). Defaults to `true`; events whose failure must never silently
   * run inline (durable migrations, iceberg resource cleanup) override to `false`. */
  allowSync?: boolean
  /** All per-tenant disable keys this concrete message matches, derived from its payload.
   * Defaults to `[type]` (v1 `shouldSend`'s default); the webhook event overrides this to add
   * `Webhook:{type}` and `Webhook:{type}:{bucket}/{name}` scoping. */
  disableKeys?: (data: QueuedPayload<T>) => string[]
}): StorageEventClass<T> {
  const version = opts.version ?? 'v1'
  class StorageEventMessage extends defineMessage<QueuedPayload<T>>({
    ...(opts.idempotencyKey !== undefined ? { idempotencyKey: opts.idempotencyKey } : {}),
    ...(opts.idempotencyTtlMs !== undefined ? { idempotencyTtlMs: opts.idempotencyTtlMs } : {}),
  }) {
    static readonly eventType = opts.type
    static readonly version = version
    static readonly allowSync = opts.allowSync ?? true
    static readonly disableKeys = opts.disableKeys
    override readonly type = opts.type

    constructor(payload: T, init?: MessageInit) {
      // Keyed by tenant: the SQS engine sends it as the group of a standard queue, so one
      // tenant's backlog does not delay the others (fair queues). System-produced messages
      // belong to no tenant and stay keyless.
      const tenant = payload.tenant?.ref
      const keyed = tenant !== undefined && tenant !== SYSTEM_TENANT_REF
      super(
        { region, ...payload, $version: payload.$version ?? version } as QueuedPayload<T>,
        keyed ? { key: tenant, ...init } : init
      )
    }

    // `this` is the concrete subclass the call was made on. The interface's `this` constraint
    // has already proven it is registry-bound; inside this generic factory that proof is not
    // expressible, hence the one cast per method.
    // `async` on purpose: a queue that is not started (or any other synchronous failure)
    // surfaces as a rejection, so `X.send(...).catch(...)` at a call site always sees it.
    // `options` is forwarded only when given — spies and stubs then see the same single
    // argument a bare `produce(message)` passes.
    static async send(payload: T, options?: StorageSendOptions): Promise<void> {
      const message = new this(payload) as BoundStorageMessage
      const queue = getQueue<StorageQueues>()
      await (options === undefined ? queue.produce(message) : queue.produce(message, options))
    }

    static async batchSend(
      payloads: ReadonlyArray<T>,
      options?: StorageSendOptions
    ): Promise<void> {
      const messages = payloads.map((payload) => new this(payload) as BoundStorageMessage)
      const queue = getQueue<StorageQueues>()
      await (options === undefined ? queue.produce(messages) : queue.produce(messages, options))
    }

    static async invoke(payload: T): Promise<void> {
      await getQueue<StorageQueues>().invoke(new this(payload) as BoundStorageMessage)
    }
  }
  return StorageEventMessage as unknown as StorageEventClass<T>
}

let storageBackend: StorageBackendAdapter | undefined = undefined

/** The worker-shared storage backend (v1 `BaseEvent.getOrCreateStorageBackend`). */
export function getOrCreateStorageBackend(): StorageBackendAdapter {
  if (storageBackend) {
    return storageBackend
  }

  const httpAgent = createAgent('s3_worker', {
    maxSockets: storageS3MaxSockets,
  })

  storageBackend = createStorageBackend(storageBackendType, {
    httpAgent,
  })

  return storageBackend
}

/** Teardown for the shared backend — wired into the queue's `onStop` (v1 `onClose`). The
 * reference is dropped before closing, so a restart during a slow close builds a fresh one. */
export async function closeStorageBackend(): Promise<void> {
  const backend = storageBackend
  storageBackend = undefined
  await backend?.close()
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

  const connection = await getPostgresConnection(connectionOptions)
  try {
    const db: Database = new StoragePgDB(connection, databaseOptions)
    return new Storage(getOrCreateStorageBackend(), db, new TenantLocation(storageS3Bucket))
  } catch (error) {
    connection.dispose()
    throw error
  }
}
