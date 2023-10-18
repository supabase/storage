import fastifyPlugin from 'fastify-plugin'
import { TenantConnection } from '../../database/connection'
import { getServiceKeyUser } from '../../database/tenant'
import { getPostgresConnection } from '../../database'
import { verifyJWT } from '../../auth'
import { logSchema } from '../../monitoring'

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
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      })
    }
    return payload
  })

  fastify.addHook('onTimeout', async (request) => {
    if (request.db) {
      try {
        await request.db.dispose()
      } catch (e) {
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      }
    }
  })

  fastify.addHook('onRequestAbort', async (request) => {
    if (request.db) {
      try {
        await request.db.dispose()
      } catch (e) {
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      }
    }
  })
})

interface DbSuperUserPluginOptions {
  disableHostCheck?: boolean
}

export const dbSuperUser = fastifyPlugin<DbSuperUserPluginOptions>(async (fastify, opts) => {
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
      disableHostCheck: opts.disableHostCheck,
    })
  })

  fastify.addHook('onSend', async (request, reply, payload) => {
    if (request.db) {
      request.db.dispose().catch((e) => {
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      })
    }

    return payload
  })

  fastify.addHook('onTimeout', async (request) => {
    if (request.db) {
      try {
        await request.db.dispose()
      } catch (e) {
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      }
    }
  })

  fastify.addHook('onRequestAbort', async (request) => {
    if (request.db) {
      try {
        await request.db.dispose()
      } catch (e) {
        logSchema.error(request.log, 'Error disposing db connection', {
          type: 'db-connection',
          error: e,
        })
      }
    }
  })
})
