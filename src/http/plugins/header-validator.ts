import { ERRORS } from '@internal/errors'
import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'

/**
 * Matches invalid HTTP header characters per RFC 7230 field-vchar specification.
 * Valid: TAB (0x09), visible ASCII (0x20-0x7E), obs-text (0x80-0xFF).
 * Invalid: control characters (0x00-0x1F except TAB) and DEL (0x7F).
 * @see https://tools.ietf.org/html/rfc7230#section-3.2
 */
const INVALID_HEADER_CHAR_PATTERN = /[^\t\x20-\x7e\x80-\xff]/

/**
 * Validates response headers before they're sent to prevent ERR_INVALID_CHAR crashes.
 *
 * Node.js throws ERR_INVALID_CHAR during writeHead() if headers contain control characters.
 * This hook validates headers in onSend (before writeHead) and throws InvalidHeaderChar error
 */
export const headerValidator = fastifyPlugin(
  async function headerValidatorPlugin(fastify: FastifyInstance) {
    fastify.addHook('onSend', async (_request: FastifyRequest, reply: FastifyReply, payload) => {
      const headers = reply.getHeaders()

      for (const [key, value] of Object.entries(headers)) {
        if (typeof value === 'string' && INVALID_HEADER_CHAR_PATTERN.test(value)) {
          throw ERRORS.InvalidHeaderChar(key, value)
        }
      }

      return payload
    })
  },
  { name: 'header-validator' }
)
