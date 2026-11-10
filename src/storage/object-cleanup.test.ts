import type { WirePayload } from '@internal/queue'
import type { AppendRecord, JobContext } from '@supabase-labs/wave-core'
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
// are free functions of events/base.
vi.mock('./events/base', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./events/base')>()),
  createStorage,
  getOrCreateStorageBackend: getBackend,
}))
// The queue engine, replaced by an in-memory backend whose `append` each test scripts (v1
// stubbed `Queue.getInstance().send`). Everything above it is the real storage queue: topic
// registry, handlers and the middleware chain (tenant gating, sync fallback).
vi.mock('@supabase-labs/wave-adapter-pgque', async (importOriginal) => {
  const { json } = await import('@supabase-labs/wave-core')
  return {
    ...(await importOriginal<typeof import('@supabase-labs/wave-adapter-pgque')>()),
    pgque: () => ({
      name: 'test-backend',
      codec: json(),
      close: async () => undefined,
      createTopic: async () => undefined,
      append,
      stats: async () => ({ subscriptions: [], topics: [] }),
    }),
  }
})
vi.mock('@internal/queue/pgque', () => ({ runPgqueMigrations: async () => undefined }))
vi.mock('@internal/queue/database', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/queue/database')>()),
  createQueuePgPool: () => ({ end: async () => undefined }),
}))

function makeCtx<T extends { tenant: { ref: string; host: string } }>(
  topic: string,
  id: string,
  data: T
): JobContext<WirePayload<T>> {
  return {
    topic,
    group: topic,
    message: { id, data: { region: 'local', ...data }, headers: {}, timestamp: 0, attempt: 1 },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

async function setup(queueEnabled = false, isMultitenant = false) {
  vi.resetModules()
  const config = await import('../config')
  config.getConfig({ reload: true })
  config.mergeConfig({
    pgQueueEnable: queueEnabled,
    // Producer-only node: appends go to the backend, handlers are inline dispatch targets.
    pgQueueEnableWorkers: false,
    pgQueueAdapter: 'pgque',
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
    storageTopics,
    TOPICS,
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
  // Disabled ⇒ the sync wave (no backend at all); enabled ⇒ a producer wave over `append`.
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
    storageTopics,
    TOPICS,
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
    expect(log).toHaveBeenCalledTimes(retryFails ? 1 : 0)
    if (retryFails) {
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
    storageTopics,
    TOPICS,
    backend,
    enqueue,
    payload,
    keys,
  } = await setup(true, true)
  getTenantConfig.mockResolvedValue({ disableEvents: [ObjectAdminDeleteBatch.eventType] })
  expect(buildHandlers().some((h) => h instanceof ObjectAdminDeleteBatchHandler)).toBe(true)
  // One topic per message class: the batch never shares a queue with single-object deletes.
  expect(storageTopics.classIndex.get(ObjectAdminDeleteBatch)).toBe(TOPICS.objectAdminDeleteBatch)
  const queues = [...storageTopics.classIndex.values()]
  expect(new Set(queues).size).toBe(queues.length)
  enqueue.mockResolvedValue(undefined)
  const remove = vi.fn().mockRejectedValue(new Error('backend unavailable'))

  await deleteObjectContent(payload, remove)

  expect(enqueue).toHaveBeenCalledOnce()
  const [topic, records] = enqueue.mock.calls[0] as [string, AppendRecord[]]
  expect(topic).toBe(TOPICS.objectAdminDeleteBatch)
  expect(records).toEqual([expect.objectContaining({ payload: expect.objectContaining(payload) })])
  const data = JSON.parse(JSON.stringify(records[0].payload)) as WirePayload<ObjectDeleteBatchEvent>
  await new ObjectAdminDeleteBatchHandler().handle(makeCtx(topic, 'job-id', data))
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
      TOPICS,
      backend,
      destroyConnection,
      payload,
      storageS3Bucket,
    } = await setup()
    await new ObjectAdminDeleteHandler().handle(
      makeCtx(TOPICS.objectAdminDelete, 'single-object-cleanup', {
        tenant: payload.tenant,
        bucketId: payload.bucketId,
        name: 'key',
        version,
      })
    )
    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(storageS3Bucket, keys)
    expect(destroyConnection).toHaveBeenCalledOnce()
  }
)

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
    TOPICS,
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
  const webhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
  const keys = ['tenant/bucket/a/v1', 'tenant/bucket/a/v1.info', 'tenant/bucket/legacy']
  backend.deleteObjectsDetailed.mockResolvedValueOnce(
    keys.map((key, i) => ({ key, outcome: i === 0 ? 'FAILED' : 'DELETED' }))
  )

  await new ObjectAdminDeleteAllBeforeHandler().handle(
    makeCtx(TOPICS.objectAdminDeleteAllBefore, 'worker-cleanup', {
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
    TOPICS,
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
  const webhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
  const error = new Error('backend unavailable')
  backend.deleteObjectsDetailed.mockRejectedValue(error)

  await expect(
    new ObjectAdminDeleteAllBeforeHandler().handle(
      makeCtx(TOPICS.objectAdminDeleteAllBefore, 'worker-cleanup', {
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
  const { ObjectAdminDeleteBatchHandler, ObjectAdminDeleteHandler, TOPICS, backend, payload } =
    await setup()
  backend.deleteObjectsDetailed.mockImplementationOnce(async (_bucket, keys) =>
    keys.map((key, i) => ({ key, outcome: i === 0 ? outcome : 'DELETED' }))
  )
  const handled =
    worker === 'ObjectAdminDelete'
      ? new ObjectAdminDeleteHandler().handle(
          makeCtx(TOPICS.objectAdminDelete, 'single-object-cleanup', {
            tenant: payload.tenant,
            bucketId: payload.bucketId,
            name: 'key',
            version: 'v1',
          })
        )
      : new ObjectAdminDeleteBatchHandler().handle(
          makeCtx(TOPICS.objectAdminDeleteBatch, 'batch-cleanup', payload)
        )

  await expect(handled).rejects.toThrow(/Backend did not confirm deletion of 1 of \d+ keys/)
  expect(backend.deleteObjects).not.toHaveBeenCalled()
})
