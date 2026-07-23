import { describe, expect, it } from 'vitest'
import buildApp from './app'
import { stripFiniteKeyword } from './http/finite'

describe('public app', () => {
  it('installs finite validation on the production Fastify instance', async () => {
    const app = buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/render/image/public/avatars/cat.png?width=1e999',
      })

      expect(response.statusCode).toBe(400)
      expect(response.json().message).toContain('finite')
    } finally {
      await app.close()
    }
  })

  it('public routes resolve correctly', async () => {
    const app = buildApp()

    try {
      const responseVersion = await app.inject({
        method: 'GET',
        url: '/version',
      })

      expect(responseVersion.statusCode).toBe(200)
      expect(responseVersion.body).toBe('0.0.0')

      const responseStatus = await app.inject({
        method: 'GET',
        url: '/status',
      })

      expect(responseStatus.statusCode).toBe(200)
      expect(responseStatus.body).toBe('')

      const responseJwks = await app.inject({
        method: 'GET',
        url: '/.well-known/jwks.json',
      })

      expect(responseJwks.statusCode).toBe(200)
      expect(responseJwks.json()).toEqual({ keys: [] })
    } finally {
      await app.close()
    }
  })

  it('does not expose the internal finite keyword in OpenAPI', async () => {
    const app = buildApp({ exposeDocs: true })

    try {
      await app.ready()

      const spec = app.swagger()
      expect(stripFiniteKeyword(spec)).toEqual(spec)

      const response = await app.inject({
        method: 'GET',
        url: '/render/image/public/avatars/cat.png?width=Infinity',
      })
      expect(response.statusCode).toBe(400)
      expect(response.json().message).toContain('finite')
    } finally {
      await app.close()
    }
  })
})
