import type { QueuedPayload, StorageJob } from '@internal/queue'
import type { BatchContext, ProduceMessage } from '@internal/queue/manager'
import { spyOnWebhooks } from '../test/utils/webhooks'
import type { ObjectDeleteBatchEvent } from './events'

vi.mock('@internal/monitoring/logger', async (importOriginal) => importOriginal())

const { getTenantConfig, createStorage, getBackend, append } = vi.hoisted(() => ({
  getTenantConfig: vi.fn(),
  createStorage: vi.fn(),
  getBackend: vi.fn(),
  append: vi.fn(),
}))
vi.mock('@internal/database', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/database')>()),
  getTenantConfig,
}))
// v1 spied on the `BaseEvent.createStorage` / `getOrCreateStorageBackend` statics; in v2 they
// are free functions of events/base. Registered per `setup()` rather than once with `vi.mock`:
// a mock factory's result outlives `vi.resetModules()`, so a file-level one would keep serving
// the first module graph's `storageEvent` — whose `send` statics close over that graph's
// `@internal/queue` singleton, not the queue the current `startStorageQueue()` started.
function mockEventsBase() {
  vi.doMock('./events/base', async (importOriginal) => ({
    ...(await importOriginal<typeof import('./events/base')>()),
    createStorage,
    getOrCreateStorageBackend: getBackend,
  }))
}
// The queue engine, replaced by a stub backend whose `send` each test scripts (v1 stubbed
// `Queue.getInstance().send`). Everything above it is the real storage queue: registry,
// handlers and the middleware chain (tenant gating, sync fallback).
vi.mock('@internal/queue/adapters/pgboss', async (importOriginal) => {
  const { json } = await import('@internal/queue/manager')
  return {
    ...(await importOriginal<typeof import('@internal/queue/adapters/pgboss')>()),
    pgboss: () => ({
      name: 'test-backend',
      codec: json(),
      close: async () => undefined,
      provision: async () => undefined,
      send: append,
      stats: async () => ({ queues: [] }),
    }),
  }
})
vi.mock('@internal/queue/boss', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/queue/boss')>()),
  createQueueBoss: () => ({ stop: async () => undefined }),
  runPgBossMigrations: async () => undefined,
}))

function makeBatchCtx<T extends { tenant: { ref: string; host: string } }>(
  queue: string,
  payloads: T[]
): BatchContext<QueuedPayload<T>> {
  return {
    queue,
    messages: payloads.map((data, i) => ({
      id: `job-${i}`,
      data: { region: 'local', ...data },
      headers: {},
      timestamp: 0,
      attempt: 1,
    })),
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

function makeCtx<T extends { tenant: { ref: string; host: string } }>(
  queue: string,
  id: string,
  data: T
): StorageJob<T> {
  return {
    queue,
    message: { id, data: { region: 'local', ...data }, headers: {}, timestamp: 0, attempt: 1 },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

async function setup(queueEnabled = false, isMultitenant = false) {
  vi.resetModules()
  mockEventsBase()
  const config = await import('../config')
  config.getConfig({ reload: true })
  config.mergeConfig({
    pgQueueEnable: queueEnabled,
    // Producer-only node: appends go to the backend, handlers are inline dispatch targets.
    pgQueueEnableWorkers: false,
    queueAdapter: 'pgboss',
    isMultitenant,
    tusUseFileVersionSeparator: false,
    storageS3Bucket: 'root',
  })
  const { logger } = await import('@internal/monitoring')
  const { TenantLocation } = await import('./locator')
  const { deleteObjectContent } = await import('./object-cleanup')
  const {
    buildHandlers,
    ObjectAdminDeleteAllBeforeHandler,
    ObjectAdminDeleteBatch,
    ObjectAdminDeleteBatchHandler,
    ObjectAdminDeleteHandler,
    ObjectRemoved,
    startStorageQueue,
    storageQueues,
    QUEUES,
  } = await import('./events')
  const location = new TenantLocation('root')
  const backend = {
    deleteObjects: vi.fn().mockResolvedValue(undefined),
    deleteObjectsDetailed: vi.fn(async (_bucket: string, keys: string[]) =>
      keys.map((key) => ({ key, outcome: 'DELETED' as 'DELETED' | 'FAILED' | 'UNKNOWN' }))
    ),
  }
  const destroyConnection = vi.fn()
  getBackend.mockReset().mockReturnValue(backend)
  createStorage.mockReset().mockResolvedValue({
    backend,
    location,
    db: { destroyConnection },
  })
  const enqueue = append.mockReset().mockRejectedValue(new Error('queue unavailable'))
  // Disabled ⇒ the sync queue (no backend at all); enabled ⇒ a producer queue over `append`.
  const queue = await startStorageQueue()
  const produce = vi.spyOn(queue, 'produce')
  const log = vi.spyOn(logger, 'error').mockImplementation(() => undefined)
  const warning = vi.spyOn(logger, 'warn').mockImplementation(() => undefined)
  const payload: ObjectDeleteBatchEvent = {
    tenant: { ref: 'tenant', host: 'localhost' },
    bucketId: 'bucket',
    objects: Array.from({ length: 500 }, (_, i) => ({ name: `key-${i}`, version: `v-${i}` })),
    reqId: 'request-id',
    sbReqId: 'sb-request-id',
  }
  const keys = payload.objects.flatMap(({ name, version }) => [
    `tenant/bucket/${name}/${version}`,
    `tenant/bucket/${name}/${version}.info`,
  ])
  return {
    storageS3Bucket: config.getConfig().storageS3Bucket,
    createStorage,
    getBackend,
    deleteObjectContent,
    location,
    buildHandlers,
    ObjectAdminDeleteAllBeforeHandler,
    ObjectAdminDeleteBatch,
    ObjectAdminDeleteBatchHandler,
    ObjectAdminDeleteHandler,
    ObjectRemoved,
    storageQueues,
    QUEUES,
    backend,
    destroyConnection,
    enqueue,
    produce,
    payload,
    keys,
    log,
    warning,
  }
}

it.each([
  { queue: 'disabled', retryFails: false },
  { queue: 'disabled', retryFails: true },
  { queue: 'unavailable', retryFails: false },
  { queue: 'unavailable', retryFails: true },
])(
  'keeps cleanup batched with queue=$queue and retryFails=$retryFails',
  async ({ queue, retryFails }) => {
    const {
      createStorage,
      getBackend,
      deleteObjectContent,
      backend,
      enqueue,
      payload,
      keys,
      log,
      warning,
    } = await setup(queue !== 'disabled')
    const error = new Error('backend unavailable')
    backend.deleteObjects.mockRejectedValueOnce(error)
    if (retryFails) backend.deleteObjectsDetailed.mockRejectedValue(error)

    const deletion = deleteObjectContent(payload, () => backend.deleteObjects('root', keys))
    if (retryFails) {
      await expect(deletion).rejects.toBe(error)
    } else {
      await expect(deletion).resolves.toBeUndefined()
    }

    expect(backend.deleteObjects).toHaveBeenCalledExactlyOnceWith('root', keys)
    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith('root', keys)
    expect(createStorage).not.toHaveBeenCalled()
    expect(getBackend).toHaveBeenCalledOnce()
    expect(enqueue).toHaveBeenCalledTimes(queue === 'disabled' ? 0 : 1)
    const summary = {
      error,
      project: 'tenant',
      type: 'storage',
      bucketId: 'bucket',
      reqId: 'request-id',
      sbReqId: 'sb-request-id',
      objectCount: 500,
    }
    expect(warning).toHaveBeenCalledWith(
      summary,
      'Backend deletion failed after commit; scheduling cleanup'
    )
    // A failed inline retry is logged twice: by the queue, for the handler run that failed,
    // and by the caller, for the cleanup it could not schedule.
    expect(log).toHaveBeenCalledTimes(retryFails ? 2 : 0)
    if (retryFails) {
      expect(log).toHaveBeenCalledWith(
        expect.objectContaining({
          event: 'ObjectAdminDeleteBatch',
          durationMs: expect.any(Number),
        }),
        expect.stringMatching(/^\[ObjectAdminDeleteBatch\]: .* - FAILED$/)
      )
      expect(log).toHaveBeenCalledWith(
        { ...summary, objects: payload.objects },
        'Failed to schedule deleted object cleanup'
      )
    }
  }
)

it('queues one batch on its registered queue even when tenant events are disabled and retries the serialized physical versions', async () => {
  const {
    deleteObjectContent,
    buildHandlers,
    ObjectAdminDeleteBatch,
    ObjectAdminDeleteBatchHandler,
    storageQueues,
    QUEUES,
    backend,
    enqueue,
    payload,
    keys,
  } = await setup(true, true)
  getTenantConfig.mockResolvedValue({ disableEvents: [ObjectAdminDeleteBatch.eventType] })
  expect(buildHandlers().some((h) => h instanceof ObjectAdminDeleteBatchHandler)).toBe(true)
  // One queue per message class: the batch never shares a queue with single-object deletes.
  expect(storageQueues.classIndex.get(ObjectAdminDeleteBatch)).toBe(QUEUES.objectAdminDeleteBatch)
  const queues = [...storageQueues.classIndex.values()]
  expect(new Set(queues).size).toBe(queues.length)
  enqueue.mockResolvedValue(undefined)
  const remove = vi.fn().mockRejectedValue(new Error('backend unavailable'))

  await deleteObjectContent(payload, remove)

  expect(enqueue).toHaveBeenCalledOnce()
  const [queue, records] = enqueue.mock.calls[0] as [string, ProduceMessage[]]
  expect(queue).toBe(QUEUES.objectAdminDeleteBatch)
  expect(records).toEqual([expect.objectContaining({ data: expect.objectContaining(payload) })])
  const data = JSON.parse(JSON.stringify(records[0].data)) as QueuedPayload<ObjectDeleteBatchEvent>
  await new ObjectAdminDeleteBatchHandler().handle(makeCtx(queue, 'job-id', data))
  expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith('root', keys)
})

it.each([
  { version: undefined, keys: ['tenant/bucket/key'] },
  {
    version: 'physical-version',
    keys: ['tenant/bucket/key/physical-version', 'tenant/bucket/key/physical-version.info'],
  },
])(
  'single-object cleanup preserves the sidecar contract (version=$version)',
  async ({ version, keys }) => {
    const {
      ObjectAdminDeleteHandler,
      QUEUES,
      backend,
      destroyConnection,
      payload,
      storageS3Bucket,
    } = await setup()
    await new ObjectAdminDeleteHandler().handleBatch(
      makeBatchCtx(QUEUES.objectAdminDelete, [
        { tenant: payload.tenant, bucketId: payload.bucketId, name: 'key', version },
      ])
    )
    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
      storageS3Bucket,
      keys,
      expect.anything()
    )
    // Content cleanup needs no tenant database connection.
    expect(destroyConnection).not.toHaveBeenCalled()
  }
)

it('deletes a batch of objects across tenants in one backend request', async () => {
  const { ObjectAdminDeleteHandler, QUEUES, backend, storageS3Bucket } = await setup()
  await new ObjectAdminDeleteHandler().handleBatch(
    makeBatchCtx(QUEUES.objectAdminDelete, [
      { tenant: { ref: 'tenant-a', host: '' }, bucketId: 'b', name: 'one', version: 'v1' },
      { tenant: { ref: 'tenant-b', host: '' }, bucketId: 'b', name: 'two' },
      // The same object twice: its keys are requested once.
      { tenant: { ref: 'tenant-a', host: '' }, bucketId: 'b', name: 'one', version: 'v1' },
    ])
  )
  expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
    storageS3Bucket,
    ['tenant-a/b/one/v1', 'tenant-a/b/one/v1.info', 'tenant-b/b/two'],
    expect.anything()
  )
})

it('fails only the messages whose keys the backend did not confirm', async () => {
  const { ObjectAdminDeleteHandler, QUEUES, backend } = await setup()
  const { BatchFailure } = await import('@internal/queue/manager')
  backend.deleteObjectsDetailed.mockImplementationOnce(async (_bucket, keys) =>
    keys.map((key) => ({ key, outcome: key.includes('/bad') ? 'FAILED' : 'DELETED' }))
  )
  const ctx = makeBatchCtx(QUEUES.objectAdminDelete, [
    { tenant: { ref: 't', host: '' }, bucketId: 'b', name: 'good' },
    { tenant: { ref: 't', host: '' }, bucketId: 'b', name: 'bad', version: 'v1' },
    { tenant: { ref: 't', host: '' }, bucketId: 'b', name: 'bad', version: 'v1' },
  ])

  const error = await new ObjectAdminDeleteHandler().handleBatch(ctx).catch((e) => e)

  expect(error).toBeInstanceOf(BatchFailure)
  expect((error as InstanceType<typeof BatchFailure>).failed).toEqual([
    ctx.messages[1],
    ctx.messages[2],
  ])
})

it('caps a pull at 500 objects so one request never exceeds 1000 keys', async () => {
  const { ObjectAdminDeleteHandler } = await setup()
  expect(new ObjectAdminDeleteHandler().options.prefetch).toBe(500)
})

it.each([false, true])(
  'does not schedule retries after successful cleanup (empty=%s)',
  async (empty) => {
    const { deleteObjectContent, produce, payload } = await setup()
    if (empty) payload.objects = []
    const remove = vi.fn().mockResolvedValue(undefined)
    await deleteObjectContent(payload, remove)
    expect(remove).toHaveBeenCalledTimes(empty ? 0 : 1)
    expect(produce).not.toHaveBeenCalled()
  }
)

it('retries the worker batch with serialized versions after its delete commits', async () => {
  const {
    createStorage,
    ObjectAdminDeleteAllBeforeHandler,
    ObjectAdminDeleteBatch,
    ObjectRemoved,
    QUEUES,
    backend,
    destroyConnection,
    location,
    produce,
    payload,
  } = await setup()
  const rows = [
    { id: 'id-1', name: 'a', version: 'v1', metadata: { size: 1 } },
    { id: 'id-2', name: 'legacy', version: null },
  ]
  const db = {
    destroyConnection,
    listObjects: vi.fn().mockResolvedValue(rows),
    deleteObjects: vi.fn().mockResolvedValue(rows),
  }
  createStorage.mockResolvedValue({ backend, location, db })
  const webhook = spyOnWebhooks(ObjectRemoved).stub()
  const keys = ['tenant/bucket/a/v1', 'tenant/bucket/a/v1.info', 'tenant/bucket/legacy']
  backend.deleteObjectsDetailed.mockResolvedValueOnce(
    keys.map((key, i) => ({ key, outcome: i === 0 ? 'FAILED' : 'DELETED' }))
  )

  await new ObjectAdminDeleteAllBeforeHandler().handle(
    makeCtx(QUEUES.objectAdminDeleteAllBefore, 'worker-cleanup', {
      tenant: payload.tenant,
      bucketId: payload.bucketId,
      before: new Date().toISOString(),
    })
  )

  expect(db.deleteObjects).toHaveBeenCalledExactlyOnceWith('bucket', ['id-1', 'id-2'], 'id')
  expect(backend.deleteObjects).not.toHaveBeenCalled()
  expect(backend.deleteObjectsDetailed.mock.calls).toEqual([
    ['root', keys],
    ['root', keys],
  ])
  expect(produce).toHaveBeenCalledOnce()
  const retry = produce.mock.calls[0][0]
  expect(retry).toBeInstanceOf(ObjectAdminDeleteBatch)
  expect(retry).toHaveProperty('data.objects', [
    { name: 'a', version: 'v1' },
    { name: 'legacy', version: null },
  ])
  expect(webhook).toHaveBeenCalledTimes(2)
  expect(destroyConnection).toHaveBeenCalledOnce()
})

it('reports a worker cleanup failure after dispatching removal webhooks', async () => {
  const {
    createStorage,
    ObjectAdminDeleteAllBeforeHandler,
    ObjectRemoved,
    QUEUES,
    backend,
    location,
    payload,
  } = await setup()
  const rows = [{ id: 'id-1', name: 'a', version: 'v1', metadata: { size: 1 } }]
  const db = {
    destroyConnection: vi.fn(),
    listObjects: vi.fn().mockResolvedValue(rows),
    deleteObjects: vi.fn().mockResolvedValue(rows),
  }
  createStorage.mockResolvedValue({ backend, location, db })
  const webhook = spyOnWebhooks(ObjectRemoved).stub()
  const error = new Error('backend unavailable')
  backend.deleteObjectsDetailed.mockRejectedValue(error)

  await expect(
    new ObjectAdminDeleteAllBeforeHandler().handle(
      makeCtx(QUEUES.objectAdminDeleteAllBefore, 'worker-cleanup', {
        tenant: payload.tenant,
        bucketId: payload.bucketId,
        before: new Date().toISOString(),
      })
    )
  ).rejects.toBe(error)
  expect(db.deleteObjects).toHaveBeenCalledOnce()
  expect(webhook).toHaveBeenCalledOnce()
  expect(db.destroyConnection).toHaveBeenCalledOnce()
})

it.each(
  (['ObjectAdminDeleteBatch', 'ObjectAdminDelete'] as const).flatMap((worker) =>
    (['FAILED', 'UNKNOWN'] as const).map((outcome) => ({ worker, outcome }))
  )
)('fails $worker when the backend reports $outcome for a key', async ({ worker, outcome }) => {
  const { ObjectAdminDeleteBatchHandler, ObjectAdminDeleteHandler, QUEUES, backend, payload } =
    await setup()
  backend.deleteObjectsDetailed.mockImplementationOnce(async (_bucket, keys) =>
    keys.map((key, i) => ({ key, outcome: i === 0 ? outcome : 'DELETED' }))
  )
  const handled =
    worker === 'ObjectAdminDelete'
      ? new ObjectAdminDeleteHandler().handleBatch(
          makeBatchCtx(QUEUES.objectAdminDelete, [
            { tenant: payload.tenant, bucketId: payload.bucketId, name: 'key', version: 'v1' },
          ])
        )
      : new ObjectAdminDeleteBatchHandler().handle(
          makeCtx(QUEUES.objectAdminDeleteBatch, 'batch-cleanup', payload)
        )

  await expect(handled).rejects.toThrow(
    worker === 'ObjectAdminDelete'
      ? /batch handler failed a subset/
      : /Backend did not confirm deletion of 1 of \d+ keys/
  )
  expect(backend.deleteObjects).not.toHaveBeenCalled()
})
