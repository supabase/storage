import { FastifyInstance, InjectOptions, LightMyRequestResponse } from 'fastify'

type HttpVerb = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD'
type InjectPayload = InjectOptions['payload']
import { anonKey, mintJWT, serviceKey } from './auth'

/**
 * Thin wrapper around fastify's `inject` that hides the repetitive
 * `headers: { authorization: 'Bearer ...' }` incantation.
 *
 *   ctx.client.asService().get(`/bucket/${b.id}`)
 *   ctx.client.asUser(user).post('/bucket', { name: 'x' })
 *   ctx.client.asAnon().delete(`/bucket/${b.id}`)
 *   ctx.client.raw({ ... })  // escape hatch for odd shapes
 *
 * Every shortcut returns fastify's `LightMyRequestResponse` unchanged so
 * tests keep full access to `.statusCode`, `.json()`, `.body`, `.headers`.
 */

export interface TestClient {
  /** Pre-bound to a service-role bearer token. */
  asService(): ScopedClient
  /** Pre-bound to a user-bound bearer token. Pass the `user.id`. */
  asUser(user: { id: string; role?: string }): Promise<ScopedClient>
  /** Pre-bound to the env anon key. */
  asAnon(): ScopedClient
  /** No auth header at all. */
  unauthenticated(): ScopedClient
  /** Escape hatch — direct passthrough to fastify `inject`. */
  raw(opts: InjectOptions): Promise<LightMyRequestResponse>
}

export interface ScopedClient {
  get(url: string, opts?: Omit<InjectOptions, 'method' | 'url'>): Promise<LightMyRequestResponse>
  post(
    url: string,
    payload?: InjectPayload,
    opts?: Omit<InjectOptions, 'method' | 'url' | 'payload'>
  ): Promise<LightMyRequestResponse>
  put(
    url: string,
    payload?: InjectPayload,
    opts?: Omit<InjectOptions, 'method' | 'url' | 'payload'>
  ): Promise<LightMyRequestResponse>
  patch(
    url: string,
    payload?: InjectPayload,
    opts?: Omit<InjectOptions, 'method' | 'url' | 'payload'>
  ): Promise<LightMyRequestResponse>
  delete(
    url: string,
    opts?: Omit<InjectOptions, 'method' | 'url'>
  ): Promise<LightMyRequestResponse>
  head(url: string, opts?: Omit<InjectOptions, 'method' | 'url'>): Promise<LightMyRequestResponse>
  /** Any other verb / complex shape. */
  inject(opts: InjectOptions): Promise<LightMyRequestResponse>
}

function makeScoped(
  app: FastifyInstance,
  baseHeaders: Record<string, string> = {}
): ScopedClient {
  const injectWith = async (
    method: HttpVerb,
    url: string,
    opts: Omit<InjectOptions, 'method' | 'url'> = {}
  ): Promise<LightMyRequestResponse> => {
    const headers = { ...baseHeaders, ...(opts.headers as Record<string, string> | undefined) }
    return app.inject({ ...opts, method, url, headers } as InjectOptions)
  }

  return {
    get: (url, opts) => injectWith('GET', url, opts),
    delete: (url, opts) => injectWith('DELETE', url, opts),
    head: (url, opts) => injectWith('HEAD', url, opts),
    post: (url, payload, opts) => injectWith('POST', url, { ...opts, payload }),
    put: (url, payload, opts) => injectWith('PUT', url, { ...opts, payload }),
    patch: (url, payload, opts) => injectWith('PATCH', url, { ...opts, payload }),
    inject: (opts) =>
      app.inject({
        ...opts,
        headers: { ...baseHeaders, ...(opts.headers as Record<string, string> | undefined) },
      }),
  }
}

export function makeClient(getApp: () => FastifyInstance): TestClient {
  return {
    asService() {
      // Service key is resolved lazily (see auth.ts) but we return a sync
      // ScopedClient by deferring the header lookup until the request is sent.
      // That keeps the test call-site one line.
      return new Proxy({} as ScopedClient, {
        get(_target, prop: keyof ScopedClient) {
          return async (...args: unknown[]) => {
            const key = await serviceKey()
            const scoped = makeScoped(getApp(), { authorization: `Bearer ${key}` })
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            return (scoped[prop] as any)(...args)
          }
        },
      })
    },
    async asUser(user) {
      const jwt = await mintJWT({ sub: user.id, role: user.role ?? 'authenticated' })
      return makeScoped(getApp(), { authorization: `Bearer ${jwt}` })
    },
    asAnon() {
      return makeScoped(getApp(), { authorization: `Bearer ${anonKey()}` })
    },
    unauthenticated() {
      return makeScoped(getApp())
    },
    raw(opts) {
      return getApp().inject(opts)
    },
  }
}
