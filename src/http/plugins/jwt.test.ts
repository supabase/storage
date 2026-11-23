import { signJWT, VERCEL_OIDC_ISSUER_PREFIX } from '@internal/auth'
import Fastify from 'fastify'
import { vi } from 'vitest'
import { setErrorHandler } from '../error-handler'

const { getJwtSecretMock } = vi.hoisted(() => ({
  getJwtSecretMock: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  getJwtSecret: getJwtSecretMock,
}))

const tenantId = 'tenant-ref'
const secret = 'jwt-plugin-test-secret-0123456789abcdef'
const vercelIssuer = `${VERCEL_OIDC_ISSUER_PREFIX}oidc/team-id`

async function buildApp() {
  getJwtSecretMock.mockResolvedValue({ secret, jwks: null })

  const { registerJwtAuth } = await import('./jwt')
  const app = Fastify()
  app.decorateRequest('tenantId', tenantId)
  setErrorHandler(app)
  registerJwtAuth(app)
  app.get('/', async (request) => ({ owner: request.owner ?? null }))
  await app.ready()
  return app
}

async function requestWithToken(payload: Record<string, unknown>) {
  const app = await buildApp()
  try {
    const token = await signJWT(payload, secret, 60)
    return await app.inject({
      method: 'GET',
      url: '/',
      headers: { authorization: `Bearer ${token}` },
    })
  } finally {
    await app.close()
  }
}

describe('JWT plugin Vercel OIDC tenant binding', () => {
  it('accepts a Vercel token whose subject is the tenant', async () => {
    const response = await requestWithToken({ iss: vercelIssuer, sub: tenantId })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({ owner: tenantId })
    expect(getJwtSecretMock).toHaveBeenCalledWith(tenantId)
  })

  it('rejects a Vercel token whose subject is another tenant', async () => {
    const response = await requestWithToken({ iss: vercelIssuer, sub: 'other-tenant-ref' })

    expect(response.statusCode).not.toBe(200)
    expect(response.json()).toMatchObject({ error: 'Unauthorized' })
  })

  it('does not bind a non-Vercel token to the tenant', async () => {
    const response = await requestWithToken({ sub: 'user-id', role: 'authenticated' })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({ owner: 'user-id' })
  })
})
