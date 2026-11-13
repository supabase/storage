import { randomUUID } from 'node:crypto'
import { DeleteObjectsCommand } from '@aws-sdk/client-s3'
import { S3Backend } from '@storage/backend/s3/adapter'
import {
  ObjectAdminDeleteAllBefore,
  ObjectAdminDeleteAllBeforeHandler,
  ObjectRemoved,
  QUEUES,
} from '@storage/events'
import * as eventsBase from '@storage/events/base'
import { Storage } from '@storage/storage'
import Fastify from 'fastify'
import { getConfig } from '../config'
import { setErrorHandler } from '../http/error-handler'
import deleteObjectsRoute from '../http/routes/object/deleteObjects'
import { authSchema } from '../http/schemas/auth'
import { errorSchema } from '../http/schemas/error'
import { useStorage } from './utils/storage'
import { spyOnWebhooks } from './utils/webhooks'

describe('Bulk delete backend compatibility', () => {
  const store = useStorage()
  const { tenantId } = getConfig()
  const names = ['deleted.txt', 'failed.txt']
  let bucketId: string
  let backend: S3Backend
  let storage: Storage
  let keys: string[]
  let blobs: Set<string>
  let failureCode: string | undefined

  beforeEach(async () => {
    bucketId = `delete-compat-${randomUUID()}`
    failureCode = 'AccessDenied'
    backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://127.0.0.1:1' })
    storage = new Storage(backend, store.database, store.storage.location)
    await store.database.createBucket({ id: bucketId, name: bucketId, public: false })
    keys = []
    for (const name of names) {
      const version = randomUUID()
      await store.database.createObject({ bucket_id: bucketId, name, version, metadata: {} })
      keys.push(storage.location.getKeyLocation({ tenantId, bucketId, objectName: name, version }))
    }
    blobs = new Set(keys)
    spyOnWebhooks(ObjectRemoved).stub()
    vi.spyOn(backend.client, 'send').mockImplementation(async (command) => {
      if (!(command instanceof DeleteObjectsCommand)) throw new Error('Unexpected S3 command')
      const deleted = []
      const errors = []
      for (const { Key } of command.input.Delete?.Objects ?? []) {
        if (Key === keys[1]) {
          if (failureCode) errors.push({ Key, Code: failureCode, Message: 'deletion failed' })
        } else if (Key !== undefined) {
          blobs.delete(Key)
          deleted.push({ Key })
        }
      }
      return { $metadata: { httpStatusCode: 200 }, Deleted: deleted, Errors: errors }
    })
  })

  afterEach(async () => {
    await store.database.deleteObjects(bucketId, names, 'name')
    await store.database.deleteBucket(bucketId)
    backend.close()
  })

  it.each([
    { label: 'AccessDenied', code: 'AccessDenied' },
    { label: 'InternalError', code: 'InternalError' },
    { label: 'no key acknowledgment', code: undefined },
  ])(
    'keeps REST metadata deletion committed for a successful S3 response with $label',
    async ({ code }) => {
      failureCode = code
      const app = Fastify()
      app.addSchema(authSchema)
      app.addSchema(errorSchema)
      setErrorHandler(app)
      app.addHook('onRequest', async (request) => {
        request.storage = storage
        request.tenantId = tenantId
      })
      await app.register(deleteObjectsRoute, { prefix: '/object' })

      try {
        const response = await app.inject({
          method: 'DELETE',
          url: `/object/${bucketId}`,
          headers: { authorization: 'Bearer test' },
          payload: { prefixes: names },
        })

        expect.soft(response.statusCode).toBe(200)
        expect
          .soft(response.json())
          .toEqual(expect.arrayContaining(names.map((name) => expect.objectContaining({ name }))))
        expect.soft([...blobs]).toEqual([keys[1]])
        expect(await store.database.listObjects(bucketId, 'name', 10)).toEqual([])
      } finally {
        await app.close()
      }
    }
  )

  it('keeps background deletion metadata committed after a per-key S3 failure', async () => {
    // The worker runs against the test's backend (v1: a subclass overriding
    // getOrCreateStorageBackend).
    const createStorage = eventsBase.createStorage
    vi.spyOn(eventsBase, 'createStorage').mockImplementation(async (payload) => {
      const workerStorage = await createStorage(payload)
      return new Storage(backend, workerStorage.db, workerStorage.location)
    })

    const error = await new ObjectAdminDeleteAllBeforeHandler()
      .handle({
        queue: QUEUES.objectAdminDeleteAllBefore,
        message: {
          id: randomUUID(),
          data: new ObjectAdminDeleteAllBefore({
            tenant: { ref: tenantId, host: 'localhost' },
            bucketId,
            before: new Date(Date.now() + 60_000).toISOString(),
          }).data,
          headers: {},
          timestamp: 0,
          attempt: 1,
        },
        signal: new AbortController().signal,
        heartbeat: async () => {},
      })
      .then(
        () => undefined,
        (error: unknown) => error
      )

    expect.soft(error).toBeUndefined()
    expect.soft([...blobs]).toEqual([keys[1]])
    expect(await store.database.listObjects(bucketId, 'name', 10)).toEqual([])
  })
})
