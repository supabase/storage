import { resolve } from 'node:path'
import { URL } from 'node:url'
import { normalizeRawError } from '@internal/errors'
import type { FastifyBaseLogger, FastifyReply, FastifyRequest } from 'fastify'
import pino, { Logger } from 'pino'
import { getConfig } from '../../config'

const {
  logLevel,
  logflareApiKey,
  logflareSourceToken,
  logflareEnabled,
  logflareBatchSize,
  region,
  version: appVersion,
} = getConfig()

export const baseLogger = pino({
  transport: buildTransport(),
  serializers: {
    error(error) {
      return normalizeRawError(error)
    },
    res(reply: Pick<FastifyReply, 'statusCode'> & Partial<Pick<FastifyReply, 'getHeaders'>>) {
      const headers = typeof reply.getHeaders === 'function' ? reply.getHeaders() : {}

      return {
        statusCode: reply.statusCode,
        headers: whitelistHeaders(headers),
      }
    },
    reqMetadata(metadata?: Record<string, unknown>) {
      if (!metadata) {
        return undefined
      }

      try {
        return JSON.stringify(metadata)
      } catch {
        // no-op
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

export let logger = baseLogger.child({ region, appVersion })

export function setLogger(newLogger: Logger) {
  logger = newLogger
}

export interface RequestLogContext {
  tenantId?: string
  project?: string
  reqId?: string
  sbReqId?: string
}

export interface RequestLog extends RequestLogContext {
  type: 'request'
  req: FastifyRequest
  res?: FastifyReply
  reqMetadata?: Record<string, unknown>
  responseTime: number
  executionTime?: number
  error?: Error | unknown
  role?: string
  owner?: string
  operation?: string
  resources?: string[]
  serverTimes?: { spanName: string; duration: number }[]
}

export interface EventLog extends RequestLogContext {
  jobId: string
  type: 'event'
  event: string
  payload: string
  objectPath: string
  resources?: string[]
}

interface ErrorLog extends RequestLogContext {
  type: string
  error?: Error | unknown
  metadata?: string
}

interface InfoLog extends RequestLogContext {
  type: string
  metadata?: string
}

export const logSchema = {
  info: (logger: FastifyBaseLogger, message: string, log: InfoLog) => logger.info(log, message),
  warning: (logger: FastifyBaseLogger, message: string, log: InfoLog | ErrorLog) =>
    logger.warn(log, message),
  request: (logger: FastifyBaseLogger, message: string, log: RequestLog) => {
    if (!log.res) {
      logger.warn(log, message)
      return
    }

    const is4xxResponse = statusOfType(log.res.statusCode, 400)
    const is5xxResponse = statusOfType(log.res.statusCode, 500)

    const logLevel = is4xxResponse ? 'warn' : is5xxResponse ? 'error' : 'info'
    logger[logLevel](log, message)
  },
  error: (logger: FastifyBaseLogger, message: string, log: ErrorLog) => logger.error(log, message),
  event: (logger: FastifyBaseLogger, message: string, log: EventLog) => logger.info(log, message),
}

export function buildTransport(): pino.TransportMultiOptions {
  const stdOutTarget = {
    level: logLevel || 'info',
    target: 'pino/file',
    // omitting options.destination logs to stdout using a worker thread
    options: {},
  }

  if (!logflareEnabled) {
    return { targets: [stdOutTarget] }
  }

  if (!logflareApiKey) {
    throw new Error('must provide a logflare api key')
  }

  if (!logflareSourceToken) {
    throw new Error('must provider a logflare source token')
  }

  const logflareModulePath = resolve(__dirname, 'logflare')

  return {
    targets: [
      stdOutTarget,
      {
        level: logLevel || 'info',
        target: 'pino-logflare',
        options: {
          apiKey: logflareApiKey,
          sourceToken: logflareSourceToken,
          batchSize: logflareBatchSize,
          onPreparePayload: { module: logflareModulePath, method: 'onPreparePayload' },
          onError: { module: logflareModulePath, method: 'onError' },
        },
      },
    ],
  }
}

const allowlistedHeaders = new Set([
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
  'cf-cache-status',
  'cf-ray',
  'location',
  'cache-control',
  'content-location',
  'content-range',
  'date',
  'transfer-encoding',
  'x-kong-proxy-latency',
  'x-kong-upstream-latency',
  'sb-request-id',
  'sb-gateway-mode',
  'sb-gateway-version',
  'x-transformations',
  'expires',
  'etag',
  'content-disposition',
  'last-modified',
])

const whitelistHeaders = (headers: Record<string, unknown>) => {
  const responseMetadata: Record<string, unknown> = {}

  for (const header in headers) {
    if (allowlistedHeaders.has(header)) {
      responseMetadata[header.replaceAll('-', '_')] = `${headers[header]}`
    }
  }

  return responseMetadata
}

export function redactQueryParamFromRequest(req: FastifyRequest, params: string[]) {
  const url = req.url
  const qIdx = url.indexOf('?')

  // Fast path: no query string, nothing to redact
  if (qIdx === -1) return url

  const query = url.slice(qIdx + 1)
  // Fast path: no sensitive params present
  if (!params.some((p) => query.includes(p))) return url

  const lUrl = new URL(url, `${req.protocol}://${req.hostname}`)
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
