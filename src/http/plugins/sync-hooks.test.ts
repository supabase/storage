import type { FastifyInstance } from 'fastify'
import Fastify from 'fastify'
import { headerValidator } from './header-validator'
import { logRequest } from './log-request'
import { httpMetrics } from './metrics'
import { requestContext } from './request-context'
import { signals as signalsPlugin } from './signals'
import { adminTenantId, tenantId } from './tenant-id'

type CapturedHook = {
  name: string
  hook: HookFunction
}

type HookFunction = (...args: unknown[]) => unknown

function captureHooks(app: FastifyInstance): CapturedHook[] {
  const hooks: CapturedHook[] = []
  const addHook = app.addHook.bind(app) as unknown as (
    name: string,
    hook: HookFunction
  ) => FastifyInstance

  vi.spyOn(app, 'addHook').mockImplementation(((name: string, hook: HookFunction) => {
    hooks.push({ name, hook })
    return addHook(name, hook)
  }) as typeof app.addHook)

  return hooks
}

async function collectHooks(register: (app: FastifyInstance) => Promise<unknown> | unknown) {
  const app = Fastify()
  const hooks = captureHooks(app)

  try {
    await register(app)
    await app.ready()
    return hooks
  } finally {
    await app.close()
  }
}

describe('sync request lifecycle hooks', () => {
  it.each([
    {
      name: 'requestContext',
      register: (app: FastifyInstance) => app.register(requestContext),
      hooks: ['onRequest'],
    },
    {
      name: 'tenantId',
      register: (app: FastifyInstance) => app.register(tenantId),
      hooks: ['onRequest'],
    },
    {
      name: 'adminTenantId',
      register: (app: FastifyInstance) => app.register(adminTenantId),
      hooks: ['onRequest'],
    },
    {
      name: 'signals',
      register: (app: FastifyInstance) => app.register(signalsPlugin),
      hooks: ['onRequest', 'onRequestAbort'],
    },
    {
      name: 'httpMetrics',
      register: (app: FastifyInstance) => app.register(httpMetrics()),
      hooks: ['onRequest', 'onResponse'],
    },
    {
      name: 'logRequest',
      register: (app: FastifyInstance) => app.register(logRequest({})),
      hooks: ['onRequest', 'preHandler', 'onSend', 'onResponse'],
    },
    {
      name: 'headerValidator',
      register: (app: FastifyInstance) => app.register(headerValidator()),
      hooks: ['onSend'],
    },
  ])('registers $name hot hooks without async functions', async ({ register, hooks }) => {
    const capturedHooks = await collectHooks(register)

    for (const hookName of hooks) {
      const hook = capturedHooks.find((candidate) => candidate.name === hookName)?.hook

      expect(hook, `${hookName} hook should be registered`).toBeDefined()
      expect(hook?.constructor.name).not.toBe('AsyncFunction')
    }
  })
})
