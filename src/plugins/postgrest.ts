import { PostgrestClient } from '@supabase/postgrest-js'
import fastifyPlugin from 'fastify-plugin'
import { getPostgrestClient } from '../utils'
import { getConfig } from '../utils/config'

declare module 'fastify' {
  interface FastifyRequest {
    postgrest: PostgrestClient
  }
}

export default fastifyPlugin(async (fastify) => {
  const { xForwardedHostRegExp } = getConfig()
  fastify.decorateRequest('postgrest', null)
  fastify.addHook('preHandler', async (request) => {
    const xForwardedHost = request.headers['x-forwarded-host']
    let postgrestURL
    if (
      typeof xForwardedHost === 'string' &&
      xForwardedHostRegExp &&
      new RegExp(xForwardedHostRegExp).test(xForwardedHost)
    ) {
      postgrestURL = `http://${xForwardedHost}/rest/v1`
    }
    request.postgrest = getPostgrestClient(request.jwt, postgrestURL)
  })
})
