import pino from 'pino'
import { getConfig } from '../utils/config'

const { logLevel } = getConfig()

export const logger = pino({
  transport: buildTransport(),
  formatters: {
    level(label, number) {
      return { level: number }
    },
  },
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
        headers: whitelistHeaders(request.headers),
        hostname: request.hostname,
        remoteAddress: request.ip,
        remotePort: request.socket.remotePort,
      }
    },
  },
  level: logLevel,
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

const whitelistHeaders = (headers: Record<string, unknown>) => {
  const responseMetadata: Record<string, unknown> = {}
  const allowlistedRequestHeaders = [
    'accept',
    'cf-connecting-ip',
    'cf-ipcountry',
    'host',
    'user-agent',
    'x-forwarded-proto',
    'referer',
    'content-length',
    'x-real-ip',
    'x-client-info',
    'x-forwarded-user-agent',
  ]
  const allowlistedResponseHeaders = [
    'cf-cache-status',
    'cf-ray',
    'content-location',
    'content-range',
    'content-type',
    'content-length',
    'date',
    'transfer-encoding',
    'x-kong-proxy-latency',
    'x-kong-upstream-latency',
    'sb-gateway-mode',
    'sb-gateway-version',
  ]
  Object.keys(headers)
    .filter(
      (header) =>
        allowlistedRequestHeaders.includes(header) || allowlistedResponseHeaders.includes(header)
    )
    .forEach((header) => {
      responseMetadata[header.replace(/-/g, '_')] = headers[header]
    })

  return responseMetadata
}
