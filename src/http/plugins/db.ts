import fastifyPlugin from 'fastify-plugin'
import { TenantConnection } from '../../database/connection'
import { getServiceKeyJwtSettings } from '../../database/tenant'
import { getPostgresConnection } from '../../database'
import { verifyJWT } from '../../auth'

declare module 'fastify' {
  interface FastifyRequest {
    db: TenantConnection
  }
}

export const db = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('db', null)
  fastify.addHook('preHandler', async (request) => {
    const adminUser = await getServiceKeyJwtSettings(request.tenantId)
    const userPayload = await verifyJWT<{ role?: string }>(request.jwt, adminUser.jwtSecret)

    request.db = await getPostgresConnection({
      user: {
        payload: userPayload,
        jwt: request.jwt,
      },
      superUser: adminUser,
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      forwardHeaders: request.headers,
      path: request.url,
      method: request.method,
    })
  })

  fastify.addHook('onResponse', async (request) => {
    await request.db.dispose()
  })
})

export const dbSuperUser = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('db', null)

  fastify.addHook('preHandler', async (request) => {
    const adminUser = await getServiceKeyJwtSettings(request.tenantId)

    request.db = await getPostgresConnection({
      user: adminUser,
      superUser: adminUser,
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      path: request.url,
      method: request.method,
    })
  })

  fastify.addHook('onResponse', async (request) => {
    await request.db.dispose()
  })
})
