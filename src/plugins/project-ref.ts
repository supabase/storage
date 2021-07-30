import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../utils/config'

declare module 'fastify' {
  interface FastifyRequest {
    projectRef: string
  }
}

export default fastifyPlugin(async (fastify) => {
  const { projectRef, xForwardedHostRegExp } = getConfig()
  fastify.decorateRequest('projectRef', projectRef)
  fastify.addHook('preHandler', async (request) => {
    if (!xForwardedHostRegExp) return
    const xForwardedHost = request.headers['x-forwarded-host']
    if (typeof xForwardedHost !== 'string') return
    const result = xForwardedHost.match(xForwardedHostRegExp)
    if (!result) return
    request.projectRef = result[1]
  })
})
