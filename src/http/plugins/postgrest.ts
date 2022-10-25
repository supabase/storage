import { getServiceKey } from '../../database/tenant'

import { PostgrestClient } from '@supabase/postgrest-js'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'
import { getPostgrestClient } from '../../database'

declare module 'fastify' {
  interface FastifyRequest {
    postgrest: PostgrestClient
    superUserPostgrest: PostgrestClient
  }
}

export const postgrest = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('postgrest', null)
  fastify.addHook('preHandler', async (request) => {
    request.postgrest = await getPostgrestClient(request.jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
    })
  })
})

export const superUserPostgrest = fastifyPlugin(async (fastify) => {
  const { isMultitenant, serviceKey } = getConfig()
  fastify.decorateRequest('superUserPostgrest', null)
  fastify.addHook('preHandler', async (request) => {
    let jwt = serviceKey
    if (isMultitenant) {
      jwt = await getServiceKey(request.tenantId)
    }
    request.superUserPostgrest = await getPostgrestClient(jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
    })
  })
})
