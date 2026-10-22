import type { Job } from 'pg-boss'
import type { ObjectDeleteBatchEvent } from './events'

vi.mock('@internal/monitoring/logger', async (importOriginal) => importOriginal())

const { getTenantConfig } = vi.hoisted(() => ({ getTenantConfig: vi.fn() }))
vi.mock('@internal/database', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/database')>()),
  getTenantConfig,
}))

async function setup(queueEnabled = false, isMultitenant = false) {
  vi.resetModules()
  const config = await import('../config')
  config.getConfig({ reload: true })
  config.mergeConfig({
    pgQueueEnable: queueEnabled,
    isMultitenant,
    tusUseFileVersionSeparator: false,
    storageS3Bucket: 'root',
  })
  const { Queue } = await import('@internal/queue')
  const { logger } = await import('@internal/monitoring')
  const { TenantLocation } = await import('./locator')
  const { deleteObjectContent } = await import('./object-cleanup')
  const {
    BaseEvent,
    ObjectAdminDeleteAllBefore,
    ObjectAdminDeleteBatch,
    ObjectAdminDelete,
    ObjectRemoved,
    registerWorkers,
  } = await import('./events')
  const location = new TenantLocation('root')
  const backend = {
    deleteObjects: vi.fn().mockResolvedValue(undefined),
    deleteObjectsDetailed: vi.fn(async (_bucket: string, keys: string[]) =>
      keys.map((key) => ({ key, outcome: 'DELETED' as 'DELETED' | 'FAILED' | 'UNKNOWN' }))
    ),
  }
  const destroyConnection = vi.fn()
  const getBackend = vi
    .spyOn(
      ObjectAdminDeleteBatch as unknown as { getOrCreateStorageBackend: () => unknown },
      'getOrCreateStorageBackend'
    )
    .mockReturnValue(backend)
  const createStorage = vi
    .spyOn(BaseEvent as unknown as { createStorage: () => Promise<unknown> }, 'createStorage')
    .mockResolvedValue({
      backend,
      location,
      db: { destroyConnection },
    })
  const enqueue = vi.fn().mockRejectedValue(new Error('queue unavailable'))
  vi.spyOn(Queue, 'getInstance').mockReturnValue({ send: enqueue } as unknown as ReturnType<
    typeof Queue.getInstance
  >)
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
    ObjectAdminDeleteAllBefore,
    ObjectAdminDeleteBatch,
    ObjectAdminDelete,
    ObjectRemoved,
    Queue,
    registerWorkers,
    backend,
    destroyConnection,
    enqueue,
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
  { queue: 'missing', retryFails: false },
  { queue: 'missing', retryFails: true },
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
    if (queue === 'missing') enqueue.mockResolvedValue(null)
    const error = new Error('backend unavailable')
    backend.deleteObjects.mockRejectedValueOnce(error)
    if (retryFails) backend.deleteObjectsDetailed.mockRejectedValue(error)

    await expect(
      deleteObjectContent(payload, () => backend.deleteObjects('root', keys))
    ).resolves.toBeUndefined()

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
    ObjectAdminDeleteBatch,
    Queue,
    registerWorkers,
    backend,
    enqueue,
    payload,
    keys,
  } = await setup(true, true)
  getTenantConfig.mockResolvedValue({ disableEvents: [ObjectAdminDeleteBatch.eventName()] })
  const register = vi.spyOn(Queue, 'register').mockImplementation(() => undefined)
  registerWorkers()
  expect(register).toHaveBeenCalledWith(ObjectAdminDeleteBatch)
  const queues = register.mock.calls.map(([event]) => event.getQueueName())
  expect(new Set(queues).size).toBe(queues.length)
  enqueue.mockResolvedValue('job-id')
  const remove = vi.fn().mockRejectedValue(new Error('backend unavailable'))

  await deleteObjectContent(payload, remove)

  expect(enqueue).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({
      name: ObjectAdminDeleteBatch.queueName,
      data: expect.objectContaining(payload),
    })
  )
  const job = JSON.parse(
    JSON.stringify({ id: 'job-id', ...enqueue.mock.calls[0][0] })
  ) as Job<ObjectDeleteBatchEvent>
  await ObjectAdminDeleteBatch.handle(job)
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
    const { ObjectAdminDelete, backend, destroyConnection, payload, storageS3Bucket } =
      await setup()
    await ObjectAdminDelete.handle({
      id: 'single-object-cleanup',
      name: ObjectAdminDelete.queueName,
      expireInSeconds: 30,
      data: { tenant: payload.tenant, bucketId: payload.bucketId, name: 'key', version },
    })
    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(storageS3Bucket, keys)
    expect(destroyConnection).toHaveBeenCalledOnce()
  }
)

it.each([false, true])(
  'does not schedule retries after successful cleanup (empty=%s)',
  async (empty) => {
    const { deleteObjectContent, ObjectAdminDeleteBatch, payload } = await setup()
    if (empty) payload.objects = []
    const send = vi.spyOn(ObjectAdminDeleteBatch, 'send')
    const remove = vi.fn().mockResolvedValue(undefined)
    await deleteObjectContent(payload, remove)
    expect(remove).toHaveBeenCalledTimes(empty ? 0 : 1)
    expect(send).not.toHaveBeenCalled()
  }
)

it('retries the worker batch with serialized versions after its delete commits', async () => {
  const {
    createStorage,
    ObjectAdminDeleteAllBefore,
    ObjectAdminDeleteBatch,
    ObjectRemoved,
    backend,
    destroyConnection,
    location,
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
  const send = vi.spyOn(ObjectAdminDeleteBatch, 'send')
  const keys = ['tenant/bucket/a/v1', 'tenant/bucket/a/v1.info', 'tenant/bucket/legacy']
  backend.deleteObjectsDetailed.mockResolvedValueOnce(
    keys.map((key, i) => ({ key, outcome: i === 0 ? 'FAILED' : 'DELETED' }))
  )

  await ObjectAdminDeleteAllBefore.handle({
    id: 'worker-cleanup',
    name: ObjectAdminDeleteAllBefore.queueName,
    expireInSeconds: 30,
    data: { tenant: payload.tenant, bucketId: payload.bucketId, before: new Date().toISOString() },
  })

  expect(db.deleteObjects).toHaveBeenCalledExactlyOnceWith('bucket', ['id-1', 'id-2'], 'id')
  expect(backend.deleteObjects).not.toHaveBeenCalled()
  expect(backend.deleteObjectsDetailed.mock.calls).toEqual([
    ['root', keys],
    ['root', keys],
  ])
  expect(send).toHaveBeenCalledExactlyOnceWith(
    expect.objectContaining({
      objects: [
        { name: 'a', version: 'v1' },
        { name: 'legacy', version: null },
      ],
    })
  )
  expect(webhook).toHaveBeenCalledTimes(2)
  expect(destroyConnection).toHaveBeenCalledOnce()
})

it.each(
  (['ObjectAdminDeleteBatch', 'ObjectAdminDelete'] as const).flatMap((worker) =>
    (['FAILED', 'UNKNOWN'] as const).map((outcome) => ({ worker, outcome }))
  )
)('fails $worker when the backend reports $outcome for a key', async ({ worker, outcome }) => {
  const { ObjectAdminDeleteBatch, ObjectAdminDelete, backend, payload } = await setup()
  backend.deleteObjectsDetailed.mockImplementationOnce(async (_bucket, keys) =>
    keys.map((key, i) => ({ key, outcome: i === 0 ? outcome : 'DELETED' }))
  )
  const job =
    worker === 'ObjectAdminDelete'
      ? {
          id: 'single-object-cleanup',
          name: ObjectAdminDelete.queueName,
          expireInSeconds: 30,
          data: { tenant: payload.tenant, bucketId: payload.bucketId, name: 'key', version: 'v1' },
        }
      : {
          id: 'batch-cleanup',
          name: ObjectAdminDeleteBatch.queueName,
          expireInSeconds: 30,
          data: payload,
        }
  const handle = worker === 'ObjectAdminDelete' ? ObjectAdminDelete : ObjectAdminDeleteBatch

  await expect(handle.handle(job as never)).rejects.toThrow(
    /Backend did not confirm deletion of 1 of \d+ keys/
  )
  expect(backend.deleteObjects).not.toHaveBeenCalled()
})
