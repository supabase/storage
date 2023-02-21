import { FastifyInstance } from 'fastify'
import fp from 'fastify-plugin'
import fastifyRateLimit from '@fastify/rate-limit'
import Redis from 'ioredis'
import { getConfig } from '../../../config'

const {
  rateLimiterDriver,
  rateLimiterRedisUrl,
  rateLimiterSkipOnError,
  rateLimiterRedisConnectTimeout,
  rateLimiterRedisCommandTimeout,
  rateLimiterRenderPathMaxReqSec,
} = getConfig()

export const rateLimiter = fp((fastify: FastifyInstance, ops: any, done: () => void) => {
  fastify.register(fastifyRateLimit, {
    global: true,
    max: rateLimiterRenderPathMaxReqSec * 4,
    timeWindow: 4 * 1000, //4s
    continueExceeding: true,
    nameSpace: 'image-transformation-ratelimit-',
    skipOnError: rateLimiterSkipOnError,
    redis:
      rateLimiterDriver === 'redis'
        ? new Redis(rateLimiterRedisUrl || '', {
            connectTimeout: rateLimiterRedisConnectTimeout * 1000,
            commandTimeout: rateLimiterRedisCommandTimeout * 1000,
          })
        : undefined,
    keyGenerator: function (request) {
      const tenant = request.tenantId
      const ip = request.headers['x-real-ip'] || request.headers['x-client-ip'] || request.ip

      // exclude query string
      const pathWithoutQuery = request.url.split('?').shift()

      return `${tenant}-${ip}-${pathWithoutQuery}`
    },
    addHeadersOnExceeding: {
      // default show all the response headers when rate limit is not reached
      'x-ratelimit-limit': false,
      'x-ratelimit-remaining': false,
      'x-ratelimit-reset': false,
    },
    addHeaders: {
      'x-ratelimit-limit': false,
      'x-ratelimit-remaining': false,
      'x-ratelimit-reset': false,
      'retry-after': false,
    },
  })

  done()
})
