import fastifyPlugin from 'fastify-plugin'
import { TenantConnection } from '../../database/connection'
import { getServiceKey } from '../../database/tenant'
import { getConfig } from '../../config'
import { getPostgresConnection } from '../../database'

declare module 'fastify' {
  interface FastifyRequest {
    db: TenantConnection
    dbSuperUser: TenantConnection
  }
}

const { isMultitenant, serviceKey } = getConfig()

export const db = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('db', null)
  fastify.addHook('preHandler', async (request) => {
    request.db = await getPostgresConnection(request.jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string | undefined,
    })
  })

  fastify.addHook('onResponse', async (request) => {
    await request.db.dispose()
  })
})

export const dbSuperUser = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('dbSuperUser', null)
  fastify.addHook('preHandler', async (request) => {
    let jwt = serviceKey
    if (isMultitenant) {
      jwt = await getServiceKey(request.tenantId)
    }

    request.dbSuperUser = await getPostgresConnection(jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
    })
  })

  fastify.addHook('onResponse', async (request) => {
    await request.db.dispose()
  })
})
