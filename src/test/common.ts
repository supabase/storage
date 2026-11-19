import { getQueue, setQueueForTesting } from '@internal/queue'
import type { FastifyInstance } from 'fastify'
import path from 'path'
import type { Mock } from 'vitest'
import { S3Backend } from '../storage/backend'

const ENV = { ...process.env }
const projectRoot = path.join(__dirname, '..', '..')

let sharedAdminAppPromise: Promise<FastifyInstance> | undefined

export async function createAdminApp(): Promise<FastifyInstance> {
  const { default: app } = await import('../admin-app')
  return app({})
}

async function getSharedAdminApp() {
  if (!sharedAdminAppPromise) {
    const appPromise = createAdminApp()
    sharedAdminAppPromise = appPromise.catch((error) => {
      if (sharedAdminAppPromise === appPromise) {
        sharedAdminAppPromise = undefined
      }

      throw error
    })
  }

  return sharedAdminAppPromise
}

type SharedAdminApp = Pick<FastifyInstance, 'inject' | 'close'>

export const adminApp: SharedAdminApp = {
  inject: ((...args: Parameters<FastifyInstance['inject']>) =>
    getSharedAdminApp().then((app) => app.inject(...args))) as FastifyInstance['inject'],
  close: (async (...args: Parameters<FastifyInstance['close']>) => {
    if (!sharedAdminAppPromise) {
      return
    }

    const appPromise = sharedAdminAppPromise
    sharedAdminAppPromise = undefined
    const app = await appPromise

    return app.close(...args)
  }) as FastifyInstance['close'],
}

export function useMockQueue() {
  beforeEach(() => {
    mockQueue()
  })
}

export function mockQueue() {
  // The v2 queue collapses send/insert into produce: both spies observe the same calls, each
  // receiving the message instance(s) passed to queue.produce.
  const sendSpy = vi.fn().mockResolvedValue(undefined)
  const invokeSpy = vi.fn().mockResolvedValue(undefined)
  const queueSpy = vi.fn().mockReturnValue({ produce: sendSpy, invoke: invokeSpy })

  setQueueForTesting({
    produce: sendSpy,
    invoke: invokeSpy,
    subscribe: vi.fn(),
    start: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
  })

  return { queueSpy, sendSpy, insertSpy: sendSpy, invokeSpy }
}

type Produce = (input: unknown, ...rest: unknown[]) => Promise<void>
type MessageOf<P> = new (...args: never[]) => { readonly data: P }
type SendSpy<P> = Mock<(payload: P) => Promise<unknown>>
interface ProduceInterception {
  /** The the queue's real produce, bound. */
  produce: Produce
  implementation: Produce
  sends: Array<{ Event: MessageOf<unknown>; send: SendSpy<never> }>
}

// Keyed by the installed produce spy. A spy restored between tests (restoreMocks) either
// leaves the queue (no entry for the real produce) or loses our implementation — both read
// as "not installed", so the next spyOnSend starts from a fresh wrapper.
const produceInterceptions = new WeakMap<object, ProduceInterception>()
// The real produce of an intercepted message, keyed by its payload object.
const passThrough = new WeakMap<object, () => Promise<void>>()

/**
 * The v2 shape of v1's `vi.spyOn(X, 'send')`: observes the `X` messages produced on the running
 * queue. The returned mock is called once per `X` message with its wire payload (`message.data`),
 * so v1 payload assertions carry over unchanged. Like the v1 spy it passes through by default
 * (on the sync queue from vitest-setup the handler then runs inline) and
 * `.mockResolvedValue(undefined)` stubs the send — of `X` only: every other message class
 * always reaches the real produce. The produce wrapper is a `vi.spyOn`, removed with the
 * test's other spies (`restoreMocks` / `vi.restoreAllMocks()`).
 */
export function spyOnSend<P extends object>(Event: MessageOf<P>): SendSpy<P> {
  const queue = getQueue() as unknown as { produce: Produce }
  const current = queue.produce as Produce & { getMockImplementation?: () => unknown }

  let interception = produceInterceptions.get(current)
  if (!interception || current.getMockImplementation?.() !== interception.implementation) {
    if (!interception && vi.isMockFunction(current)) {
      throw new Error('spyOnSend needs the real queue: produce is already mocked (mockQueue?)')
    }

    const produce = interception?.produce ?? current.bind(queue)
    const sends: ProduceInterception['sends'] = []
    const sendOf = (message: unknown) => sends.find(({ Event }) => message instanceof Event)?.send
    const implementation: Produce = async (input, ...rest) => {
      const messages: unknown[] = Array.isArray(input) ? input : [input]
      if (!messages.some(sendOf)) {
        return produce(input, ...rest)
      }

      await Promise.all(
        messages.map((message) => {
          const send = sendOf(message)
          if (!send) {
            return produce(message, ...rest)
          }
          const payload = (message as { data: object }).data
          passThrough.set(payload, () => produce(message, ...rest))
          return send(payload as never)
        })
      )
    }

    vi.spyOn(queue, 'produce').mockImplementation(implementation)
    interception = { produce, implementation, sends }
    produceInterceptions.set(queue.produce, interception)
  }

  const existing = interception.sends.find((entry) => entry.Event === Event)
  if (existing) {
    return existing.send as unknown as SendSpy<P>
  }

  const send: SendSpy<P> = vi.fn(async (payload: P) => passThrough.get(payload)?.())
  interception.sends.push({
    Event: Event as MessageOf<unknown>,
    send: send as unknown as SendSpy<never>,
  })
  return send
}

export function useMockObject() {
  beforeEach(() => {
    process.env = { ...ENV }

    vi.spyOn(S3Backend.prototype, 'getObject').mockResolvedValue({
      metadata: {
        httpStatusCode: 200,
        size: 3746,
        mimetype: 'image/png',
        lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
        eTag: 'abc',
        cacheControl: 'no-cache',
        contentLength: 3746,
      },
      httpStatusCode: 200,
      body: Buffer.from(''),
    })

    vi.spyOn(S3Backend.prototype, 'uploadObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
      cacheControl: 'no-cache',
      contentLength: 3746,
    })

    vi.spyOn(S3Backend.prototype, 'copyObject').mockResolvedValue({
      httpStatusCode: 200,
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
    })

    vi.spyOn(S3Backend.prototype, 'deleteObject').mockResolvedValue()

    vi.spyOn(S3Backend.prototype, 'deleteObjectsDetailed').mockImplementation(
      async (_bucket, keys) => keys.map((key) => ({ key, outcome: 'DELETED' as const }))
    )

    vi.spyOn(S3Backend.prototype, 'headObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      eTag: 'abc',
      cacheControl: 'no-cache',
      lastModified: new Date('Wed, 12 Oct 2022 11:17:02 GMT'),
      contentLength: 3746,
    })

    vi.spyOn(S3Backend.prototype, 'privateAssetUrl').mockResolvedValue(
      `local:///${projectRoot}/data/sadcat.jpg`
    )
  })
}
