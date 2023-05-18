import fastifyPlugin from 'fastify-plugin'
import { TenantConnection } from '../../database/connection'
import { getServiceKeyUser } from '../../database/tenant'
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
    const adminUser = await getServiceKeyUser(request.tenantId)
    const userPayload = await verifyJWT<{ role?: string }>(request.jwt, adminUser.jwtSecret)

    request.db = await getPostgresConnection({
      user: {
        payload: userPayload,
        jwt: request.jwt,
      },
      superUser: adminUser,
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      headers: request.headers,
      path: request.url,
      method: request.method,
    })
  })

  fastify.addHook('onSend', async (request, reply, payload) => {
    if (request.db) {
      request.db.dispose().catch((e) => {
        request.log.error(e, 'Error disposing db connection')
      })
    }
    return payload
  })

  fastify.addHook('onTimeout', async (request) => {
    if (request.db) {
      await request.db.dispose()
    }
  })

  fastify.addHook('onRequestAbort', async (request) => {
    if (request.db) {
      await request.db.dispose()
    }
  })
})

export const dbSuperUser = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('db', null)

  fastify.addHook('preHandler', async (request) => {
    const adminUser = await getServiceKeyUser(request.tenantId)

    request.db = await getPostgresConnection({
      user: adminUser,
      superUser: adminUser,
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      path: request.url,
      method: request.method,
      headers: request.headers,
    })
  })

  fastify.addHook('onSend', async (request, reply, payload) => {
    if (request.db) {
      request.db.dispose().catch((e) => {
        request.log.error(e, 'Error disposing db connection')
      })
    }

    return payload
  })

  fastify.addHook('onTimeout', async (request) => {
    if (request.db) {
      await request.db.dispose()
    }
  })

  fastify.addHook('onRequestAbort', async (request) => {
    if (request.db) {
      await request.db.dispose()
    }
  })
})
