import { ERRORS } from '@internal/errors'
import { hasInvalidHeaderValueChars } from '@internal/http/header'
import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'

interface HeaderValidatorOptions {
  excludeUrls?: string[]
}

/**
 * Validates response headers before they're sent to prevent ERR_INVALID_CHAR crashes.
 *
 * Node.js throws ERR_INVALID_CHAR during writeHead() if headers contain control characters.
 * This hook validates headers in onSend (before writeHead) and throws InvalidHeaderChar error
 */
export const headerValidator = (options: HeaderValidatorOptions = {}) =>
  fastifyPlugin(
    async function headerValidatorPlugin(fastify: FastifyInstance) {
      fastify.addHook('onSend', async (request: FastifyRequest, reply: FastifyReply, payload) => {
        if (options.excludeUrls?.includes(request.url.toLowerCase())) {
          return payload
        }

        const headers = reply.getHeaders()
        for (const key in headers) {
          if (!Object.prototype.hasOwnProperty.call(headers, key)) {
            continue
          }
          const value = headers[key]
          if (typeof value === 'string') {
            if (hasInvalidHeaderValueChars(value)) {
              throw ERRORS.InvalidHeaderChar(key, value)
            }
          } else if (Array.isArray(value)) {
            for (let j = 0; j < value.length; j++) {
              const item = value[j]
              if (typeof item === 'string' && hasInvalidHeaderValueChars(item)) {
                throw ERRORS.InvalidHeaderChar(key, item)
              }
            }
          }
        }

        return payload
      })
    },
    { name: 'header-validator' }
  )
