import { PostgrestClient } from '@supabase/postgrest-js'
import fastifyPlugin from 'fastify-plugin'
import { getPostgrestClient } from '../utils'

declare module 'fastify' {
  interface FastifyRequest {
    postgrest: PostgrestClient
  }
}

export default fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('postgrest', null)
  fastify.addHook('preHandler', async (request) => {
    request.postgrest = getPostgrestClient(request.jwt)
  })
})
