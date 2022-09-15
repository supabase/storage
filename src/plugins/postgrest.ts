import { getAnonKey, getServiceKey } from '../utils/tenant'

import { FastifyRequest } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../utils/config'
import { generateForwardHeaders } from '../utils/plugins'

declare module 'fastify' {
  interface FastifyRequest {
    postgrest: PostgrestClient
    superUserPostgrest: PostgrestClient
  }
}

async function getPostgrestClient(request: FastifyRequest, jwt: string): Promise<PostgrestClient> {
  const {
    anonKey,
    isMultitenant,
    postgrestURL,
    postgrestURLScheme,
    postgrestURLSuffix,
    xForwardedHostRegExp,
    forwardHeaders
  } = getConfig()

  let url = postgrestURL
  let apiKey = anonKey
  if (isMultitenant && xForwardedHostRegExp) {
    const xForwardedHost = request.headers['x-forwarded-host']
    if (typeof xForwardedHost !== 'string') {
      throw new Error('X-Forwarded-Host header is not a string')
    }
    if (!new RegExp(xForwardedHostRegExp).test(xForwardedHost)) {
      throw new Error('X-Forwarded-Host header does not match regular expression')
    }
    url = `${postgrestURLScheme}://${xForwardedHost}${postgrestURLSuffix}`
    apiKey = await getAnonKey(request.tenantId)
  }

  const pgClientHeaders = {
    apiKey,
    Authorization: `Bearer ${jwt}`
  }
  return new PostgrestClient(url, {
    headers: {
      ...pgClientHeaders, // hard coded headers
      ...generateForwardHeaders(forwardHeaders, request, Object.keys(pgClientHeaders)) // extra forwarded headers
    },
    schema: 'storage',
  })
}

export const postgrest = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('postgrest', null)
  fastify.addHook('preHandler', async (request) => {
    request.postgrest = await getPostgrestClient(request, request.jwt)
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
    request.superUserPostgrest = await getPostgrestClient(request, jwt)
  })
})
