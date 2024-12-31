import pino, { BaseLogger } from 'pino'
import { getConfig } from '../../config'
import { FastifyReply, FastifyRequest } from 'fastify'
import { URL } from 'node:url'
import { normalizeRawError } from '@internal/errors'

const { logLevel, logflareApiKey, logflareSourceToken, logflareEnabled, region } = getConfig()

export const baseLogger = pino({
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
        region,
        traceId: request.id,
        method: request.method,
        url: redactQueryParamFromRequest(request, [
          'token',
          'X-Amz-Credential',
          'X-Amz-Signature',
          'X-Amz-Security-Token',
        ]),
        headers: whitelistHeaders(request.headers),
        hostname: request.hostname,
        remoteAddress: request.ip,
        remotePort: request.socket?.remotePort,
      }
    },
  },
  level: logLevel,
  timestamp: pino.stdTimeFunctions.isoTime,
})

export const logger = baseLogger.child({ region })

export interface RequestLog {
  type: 'request'
  req: FastifyRequest
  res?: FastifyReply
  responseTime: number
  error?: Error | unknown
  role?: string
  owner?: string
  operation?: string
  resources?: string[]
  serverTimes?: { spanName: string; duration: number }[]
}

export interface EventLog {
  jodId: string
  type: 'event'
  event: string
  payload: string
  objectPath: string
  tenantId: string
  project: string
  resources?: string[]
  reqId?: string
}

interface ErrorLog {
  type: string
  error?: Error | unknown
  project?: string
  metadata?: string
}

interface InfoLog {
  type: string
  project?: string
}

export const logSchema = {
  info: (logger: BaseLogger, message: string, log: InfoLog) => logger.info(log, message),
  warning: (logger: BaseLogger, message: string, log: InfoLog | ErrorLog) =>
    logger.warn(log, message),
  request: (logger: BaseLogger, message: string, log: RequestLog) => {
    if (!log.res) {
      logger.warn(log, message)
      return
    }

    const is4xxResponse = statusOfType(log.res.statusCode, 400)
    const is5xxResponse = statusOfType(log.res.statusCode, 500)

    const logLevel = is4xxResponse ? 'warn' : is5xxResponse ? 'error' : 'info'
    logger[logLevel](log, message)
  },
  error: (logger: BaseLogger, message: string, log: ErrorLog) => logger.error(log, message),
  event: (logger: BaseLogger, message: string, log: EventLog) => logger.info(log, message),
}

export function buildTransport(): pino.TransportMultiOptions | undefined {
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
    'x-forwarded-prefix',
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
    'upload-offset',
    'tus-resumable',
    'range',
  ]
  const allowlistedResponseHeaders = [
    'cf-cache-status',
    'cf-ray',
    'location',
    'cache-control',
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
    'x-transformations',
    'expires',
    'etag',
    'content-disposition',
    'last-modified',
  ]
  Object.keys(headers)
    .filter(
      (header) =>
        allowlistedRequestHeaders.includes(header) || allowlistedResponseHeaders.includes(header)
    )
    .forEach((header) => {
      responseMetadata[header.replace(/-/g, '_')] = `${headers[header]}`
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

function statusOfType(statusCode: number, ofType: number) {
  return statusCode >= ofType && statusCode < ofType + 100
}
