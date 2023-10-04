import pino, { BaseLogger } from 'pino'
import { getConfig } from '../config'
import { FastifyReply, FastifyRequest } from 'fastify'
import { URL } from 'url'
import { normalizeRawError } from '../storage'

const { logLevel } = getConfig()

export const logger = pino({
  transport: buildTransport(),
  serializers: {
    error(error) {
      return normalizeRawError(error)
    },
    res(reply) {
      return {
        statusCode: reply.statusCode,
        headers: whitelistHeaders(reply.getHeaders()),
      }
    },
    req(request) {
      return {
        traceId: request.id,
        method: request.method,
        url: redactQueryParamFromRequest(request, ['token']),
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

export interface RequestLog {
  type: 'request'
  req: FastifyRequest
  res: FastifyReply
  responseTime: number
  error?: Error | unknown
  owner?: string
}

export interface EventLog {
  jodId: string
  type: 'event'
  event: string
  payload: string
  objectPath: string
  tenantId: string
  project: string
  reqId?: string
}

interface ErrorLog {
  type: string
  error?: Error | unknown
}

export const logSchema = {
  request: (logger: BaseLogger, message: string, log: RequestLog) => {
    logger.info(log, message)
  },
  error: (logger: BaseLogger, message: string, log: ErrorLog) => logger.error(log, message),
  event: (logger: BaseLogger, message: string, log: EventLog) => logger.info(log, message),
}

export function buildTransport(): pino.TransportMultiOptions | undefined {
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
    targets: [
      {
        level: logLevel || 'info',
        target: './logflare',
        options: {},
      },
      {
        level: logLevel || 'info',
        target: 'pino/file',
        options: {},
      },
    ],
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
    'x-forwarded-host',
    'x-forwarded-port',
    'referer',
    'content-length',
    'x-real-ip',
    'x-client-info',
    'x-forwarded-user-agent',
    'x-client-trace-id',
    'x-upsert',
    'content-type',
    'if-none-match',
    'if-modified-since',
    'upload-metadata',
    'upload-length',
    'tus-resumable',
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

export function redactQueryParamFromRequest(req: FastifyRequest, params: string[]) {
  const lUrl = new URL(req.url, `${req.protocol}://${req.hostname}`)

  params.forEach((param) => {
    if (lUrl.searchParams.has(param)) {
      lUrl.searchParams.set(param, 'redacted')
    }
  })
  return `${lUrl.pathname}${lUrl.search}`
}
