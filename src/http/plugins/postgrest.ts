import { getServiceKey } from '../../database/tenant'
import { PostgrestClient } from '@supabase/postgrest-js'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'
import { getPostgrestClient } from '../../database'
import { IncomingHttpHeaders } from 'http'

declare module 'fastify' {
  interface FastifyRequest {
    postgrest: PostgrestClient
    superUserPostgrest: PostgrestClient
  }
}

const { isMultitenant, serviceKey } = getConfig()

export const postgrest = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('postgrest', null)
  fastify.addHook('preHandler', async (request) => {
    const { postgrestForwardHeaders } = getConfig()

    request.postgrest = await getPostgrestClient(request.jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      forwardHeaders: whitelistPostgrestHeaders(postgrestForwardHeaders, request.headers),
    })
  })
})

export const superUserPostgrest = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('superUserPostgrest', null)
  fastify.addHook('preHandler', async (request) => {
    let jwt = serviceKey
    if (isMultitenant) {
      jwt = await getServiceKey(request.tenantId)
    }
    const { postgrestForwardHeaders } = getConfig()

    request.superUserPostgrest = await getPostgrestClient(jwt, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      forwardHeaders: whitelistPostgrestHeaders(postgrestForwardHeaders, request.headers),
    })
  })
})

function whitelistPostgrestHeaders(
  allowedHeaders: string | undefined,
  headers: IncomingHttpHeaders
): Record<string, string> {
  if (!allowedHeaders) {
    return {}
  }

  return allowedHeaders
    .split(',')
    .map((headerName) => headerName.trim())
    .reduce((extraHeaders, headerName) => {
      const headerValue = headers[headerName]
      if (typeof headerValue !== 'string') {
        throw new Error(`header ${headerName} must be string`)
      }
      extraHeaders[headerName] = headerValue
      return extraHeaders
    }, {} as Record<string, string>)
}
