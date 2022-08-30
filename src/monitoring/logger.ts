import pino from 'pino'
import { getConfig } from '../utils/config'

export const logger = pino({
  transport: buildTransport(),
  formatters: {
    level(label) {
      return { level: label }
    },
  },
  redact: ['req.headers.authorization', 'req.headers.apikey', 'req.headers["api-key"]'],
  serializers: {
    res(reply) {
      return {
        url: reply.url,
        statusCode: reply.statusCode,
      }
    },
    req(request) {
      return {
        method: request.method,
        url: request.url,
        headers: request.headers,
        hostname: request.hostname,
        remoteAddress: request.ip,
        remotePort: request.socket.remotePort,
      }
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
})

export function buildTransport(): pino.TransportSingleOptions | undefined {
  const { logflareApiKey, logflareSourceToken, logflareEnabled } = getConfig()

  if (!logflareEnabled) {
    return undefined
  }

  if (!logflareApiKey) {
    throw new Error('must provide a logflare api key')
  }

  if (!logflareSourceToken) {
    throw new Error('must provider a logflare source token')
  }

  return {
    target: './logflare',
  }
}
