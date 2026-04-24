import { text } from 'node:stream/consumers'
import { ERRORS } from '@internal/errors'
import { logSchema } from '@internal/monitoring'
import { FastifyRequest } from 'fastify'
import { Readable } from 'stream'
import { Agent, Dispatcher, interceptors } from 'undici'
import { getConfig } from '../../config'
import { StorageBackendAdapter } from '../backend'
import { AssetMetadata, Renderer, RenderOptions } from './renderer'

/**
 * All the transformations options available
 */
export interface TransformOptions {
  width?: number
  height?: number
  resize?: 'cover' | 'contain' | 'fill'
  format?: 'origin' | 'avif' | 'webp'
  quality?: number
}

const {
  imgLimits,
  imgProxyHttpKeepAlive,
  imgProxyHttpMaxSockets,
  imgProxyURL,
  imgProxyRequestTimeout,
} = getConfig()

const LIMITS = {
  height: {
    min: imgLimits.size.min,
    max: imgLimits.size.max,
  },
  width: {
    min: imgLimits.size.min,
    max: imgLimits.size.max,
  },
}

const IMGPROXY_REQUEST_TIMEOUT_MS = imgProxyRequestTimeout * 1000
const IMGPROXY_MAX_RETRIES = 5
const IMGPROXY_RETRY_MIN_TIMEOUT_MS = 50
const IMGPROXY_RETRY_MAX_TIMEOUT_MS = 1000
const IMGPROXY_RETRY_TIMEOUT_FACTOR = 2
const IMGPROXY_RETRY_AFTER_ENABLED = true
const IMGPROXY_RETRY_DELAY_BUDGET_MS = IMGPROXY_MAX_RETRIES * IMGPROXY_RETRY_MAX_TIMEOUT_MS
// Bound the full request lifetime (DNS/connect/TLS/headers/body/streaming) since
// dispatcher headers/body timeouts only cover idle phases. Include retry sleeps
// so the final retry still gets a full per-attempt timeout window.
const IMGPROXY_TOTAL_TIMEOUT_MS =
  IMGPROXY_REQUEST_TIMEOUT_MS * (IMGPROXY_MAX_RETRIES + 1) + IMGPROXY_RETRY_DELAY_BUDGET_MS
const IMAGE_RENDERER_RESPONSE_HEADERS = ['content-length', 'content-type', 'last-modified'] as const

const dispatcher: Dispatcher = new Agent({
  bodyTimeout: IMGPROXY_REQUEST_TIMEOUT_MS,
  headersTimeout: IMGPROXY_REQUEST_TIMEOUT_MS,
  keepAliveMaxTimeout: imgProxyHttpKeepAlive * 1000,
  keepAliveTimeout: 2 * 1000,
  ...(imgProxyHttpMaxSockets > 0 ? { connections: imgProxyHttpMaxSockets } : {}),
}).compose(
  interceptors.retry({
    maxRetries: IMGPROXY_MAX_RETRIES,
    maxTimeout: IMGPROXY_RETRY_MAX_TIMEOUT_MS,
    methods: ['GET'],
    minTimeout: IMGPROXY_RETRY_MIN_TIMEOUT_MS,
    retryAfter: IMGPROXY_RETRY_AFTER_ENABLED,
    throwOnError: false,
    timeoutFactor: IMGPROXY_RETRY_TIMEOUT_FACTOR,
    statusCodes: [408, 429, 500, 502, 503, 504],
    errorCodes: [
      'ECONNRESET',
      'ECONNREFUSED',
      'ETIMEDOUT',
      'ENOTFOUND',
      'ENETDOWN',
      'ENETUNREACH',
      'EHOSTDOWN',
      'EHOSTUNREACH',
      'EPIPE',
      'UND_ERR_CONNECT_TIMEOUT',
      'UND_ERR_SOCKET',
      'UND_ERR_HEADERS_TIMEOUT',
      'UND_ERR_BODY_TIMEOUT',
    ],
  })
)

interface TransformLimits {
  maxResolution?: number | null
}

interface ImageRendererRequestOptions {
  signal?: AbortSignal
  headers?: Record<string, string | string[] | null | undefined>
}

interface ImageRendererResponse {
  data?: Readable
  status: number
  headers: Record<string, string | undefined>
}

interface ImageRendererClient {
  get(url: string, options?: ImageRendererRequestOptions): Promise<ImageRendererResponse>
}

class ImageRendererRequestError extends Error {
  readonly originalError?: unknown

  private constructor(
    message: string,
    readonly response?: ImageRendererResponse,
    originalError?: unknown,
    cause?: unknown
  ) {
    super(message, cause === undefined ? undefined : { cause })
    this.name = 'ImageRendererRequestError'
    this.originalError = originalError
  }

  static fromResponse(response: ImageRendererResponse) {
    return new ImageRendererRequestError(
      `Request failed with status code ${response.status}`,
      response
    )
  }

  static forFailure(error: unknown) {
    return new ImageRendererRequestError(formatRequestErrorMessage(error), undefined, error)
  }

  static forAbortRace(error: unknown, abortReason: unknown) {
    return new ImageRendererRequestError(
      formatRequestErrorMessage(error),
      undefined,
      error,
      abortReason
    )
  }
}

const client: ImageRendererClient = {
  async get(url, options = {}) {
    const requestUrl = resolveImgProxyUrl(url)
    // bound the total request lifetime (DNS/connect/TLS/headers/body/streaming)
    // since dispatcher headersTimeout/bodyTimeout only cover idle phases.
    const totalTimeoutSignal = AbortSignal.timeout(IMGPROXY_TOTAL_TIMEOUT_MS)
    const fetchSignal =
      options.signal instanceof AbortSignal
        ? AbortSignal.any([options.signal, totalTimeoutSignal])
        : totalTimeoutSignal

    let response: Response
    try {
      response = await fetch(requestUrl, {
        method: 'GET',
        headers: createHeaders(options.headers),
        signal: fetchSignal,
        dispatcher,
      } as RequestInit & { dispatcher?: Dispatcher })
    } catch (e) {
      if (options.signal?.aborted) {
        if (e === options.signal.reason) {
          throw e
        }

        throw ImageRendererRequestError.forAbortRace(e, options.signal.reason)
      }

      if (totalTimeoutSignal.aborted) {
        throw ImageRendererRequestError.forFailure(buildTotalTimeoutError())
      }

      throw ImageRendererRequestError.forFailure(e)
    }

    if (options.signal?.aborted) {
      await cancelResponseBody(response)
      throw getAbortReason(options.signal, new DOMException('aborted', 'AbortError'))
    }

    if (totalTimeoutSignal.aborted) {
      await cancelResponseBody(response)
      throw ImageRendererRequestError.forFailure(buildTotalTimeoutError())
    }

    const inStreamSignals = [options.signal, totalTimeoutSignal] as const

    if (response.ok) {
      return toImageRendererResponse(response, inStreamSignals)
    }

    throw ImageRendererRequestError.fromResponse(toImageRendererResponse(response, inStreamSignals))
  },
}

/**
 * ImageRenderer
 * renders an image by applying transformations
 *
 * Interacts with an imgproxy backend for the actual transformation
 */
export class ImageRenderer extends Renderer {
  private readonly client: ImageRendererClient
  private transformOptions?: TransformOptions
  private limits?: TransformLimits

  constructor(private readonly backend: StorageBackendAdapter) {
    super()
    this.client = client
  }

  /**
   * Applies whitelisted transformations with specific limits applied
   * @param options
   * @param keepOriginal
   */
  static applyTransformation(options: TransformOptions, keepOriginal?: boolean): string[] {
    const segments = []

    if (options.height) {
      segments.push(`height:${clamp(options.height, LIMITS.height.min, LIMITS.height.max)}`)
    }

    if (options.width) {
      segments.push(`width:${clamp(options.width, LIMITS.width.min, LIMITS.width.max)}`)
    }

    if (options.width || options.height) {
      if (keepOriginal) {
        segments.push(`resize:${options.resize}`)
      } else {
        segments.push(`resizing_type:${this.formatResizeType(options.resize)}`)
      }
    }

    if (options.quality) {
      segments.push(`quality:${options.quality}`)
    }

    if (options.format && options.format !== 'origin') {
      segments.push(`format:${options.format}`)
    }

    return segments
  }

  static applyTransformationLimits(limits: TransformLimits) {
    const transforms: string[] = []
    if (typeof limits?.maxResolution === 'number') {
      transforms.push(`max_src_resolution:${limits.maxResolution}`)
    }

    return transforms
  }

  protected static formatResizeType(resize: TransformOptions['resize']) {
    const defaultResize = 'fill'

    switch (resize) {
      case 'cover':
        return defaultResize
      case 'contain':
        return 'fit'
      case 'fill':
        return 'force'
      default:
        return defaultResize
    }
  }

  /**
   * Get the base http client
   */
  getClient() {
    return this.client
  }

  /**
   * Set transformations parameters before calling the render method
   * @param transformations
   */
  setTransformations(transformations: TransformOptions) {
    this.transformOptions = transformations
    return this
  }

  setLimits(limits: TransformLimits) {
    this.limits = limits
    return this
  }

  setTransformationsFromString(transformations: string) {
    const transformOptions: TransformOptions = {}

    for (const param of transformations.split(',')) {
      const [name, value] = param.split(':')
      if (value === undefined || value === '') {
        continue
      }

      switch (name) {
        case 'height':
          transformOptions.height = parseInt(value, 10)
          break
        case 'width':
          transformOptions.width = parseInt(value, 10)
          break
        case 'resize':
          transformOptions.resize = value as TransformOptions['resize']
          break
        case 'format':
          transformOptions.format = value as TransformOptions['format']
          break
        case 'quality':
          transformOptions.quality = parseInt(value, 10)
          break
      }
    }

    this.transformOptions = transformOptions

    return this
  }

  /**
   * Fetch the transformed asset from imgproxy.
   * We use a secure signed url in order for imgproxy to download and
   * transform the image
   * @param request
   * @param options
   */
  async getAsset(request: FastifyRequest, options: RenderOptions) {
    const [privateURL, headObj] = await Promise.all([
      this.backend.privateAssetUrl(options.bucket, options.key, options.version),
      this.backend.headObject(options.bucket, options.key, options.version),
    ])
    const transformations = ImageRenderer.applyTransformation(this.transformOptions || {})
    const transformLimits = ImageRenderer.applyTransformationLimits(this.limits || {})

    const url = [
      '/public',
      ...transformations,
      ...transformLimits,
      'plain',
      privateURL.startsWith('local://') ? privateURL : encodeURIComponent(privateURL),
    ]
    let assetBody: Readable | undefined

    try {
      const acceptHeader =
        this.transformOptions?.format !== 'origin' ? request.headers['accept'] : undefined

      const response = await this.getClient().get(url.join('/'), {
        signal: options.signal,
        headers: acceptHeader
          ? {
              accept: acceptHeader,
            }
          : undefined,
      })
      assetBody = response.data

      const rawContentLength = response.headers['content-length']
      let contentLength: number | undefined
      if (rawContentLength !== undefined) {
        const parsedContentLength = Number(rawContentLength)
        if (/^\d+$/.test(rawContentLength) && Number.isSafeInteger(parsedContentLength)) {
          contentLength = parsedContentLength
        } else {
          logInvalidContentLength(request, rawContentLength)
        }
      }
      const lastModified = parseLastModifiedHeader(response.headers['last-modified'])
      const metadata: AssetMetadata = {
        httpStatusCode: response.status,
        lastModified,
        eTag: headObj.eTag,
        cacheControl: headObj.cacheControl,
        mimetype: response.headers['content-type'],
        ...(contentLength !== undefined
          ? {
              contentLength,
              size: contentLength,
            }
          : {}),
      }

      return {
        body: assetBody,
        transformations,
        metadata,
      }
    } catch (e) {
      if (e instanceof ImageRendererRequestError) {
        const error = await this.handleRequestError(e, options.signal)
        throw error.withMetadata({
          transformations,
        })
      }

      assetBody?.destroy()
      throw e
    }
  }

  protected async handleRequestError(error: ImageRendererRequestError, signal?: AbortSignal) {
    const stream = error.response?.data
    if (!stream) {
      return ERRORS.InternalError(error, error.message)
    }

    let errorResponse: string
    try {
      errorResponse = await text(stream)
    } catch (e) {
      if (signal?.aborted) {
        throw getAbortReason(signal, e)
      }

      return ERRORS.InternalError(e instanceof Error ? e : undefined, formatRequestErrorMessage(e))
    }

    const statusCode = error.response?.status || 500
    return ERRORS.ImageProcessingError(statusCode, errorResponse || error.message)
  }
}

function createHeaders(headers?: ImageRendererRequestOptions['headers']) {
  const result = new Headers()

  for (const [name, value] of Object.entries(headers || {})) {
    if (value == null) {
      continue
    }

    result.set(name, Array.isArray(value) ? value.join(', ') : value)
  }

  return result
}

function resolveImgProxyUrl(url: string) {
  if (isAbsoluteUrl(url) || !imgProxyURL) {
    return url
  }

  return `${imgProxyURL.replace(/\/+$/, '')}/${url.replace(/^\/+/, '')}`
}

function isAbsoluteUrl(url: string) {
  return /^[a-z][a-z\d+\-.]*:\/\//i.test(url)
}

function toImageRendererResponse(
  response: Response,
  abortSignals: ReadonlyArray<AbortSignal | undefined>
): ImageRendererResponse {
  let data: Readable
  try {
    data = response.body ? readableFromWeb(response.body, abortSignals) : Readable.from([])
  } catch (error) {
    void cancelResponseBody(response)
    throw error
  }

  return {
    data,
    status: response.status,
    headers: imageRendererResponseHeaders(response.headers),
  }
}

function readableFromWeb(
  body: ReadableStream<Uint8Array>,
  abortSignals: ReadonlyArray<AbortSignal | undefined>
): Readable {
  const reader = body.getReader()
  let reading = false
  let released = false

  const releaseLock = () => {
    if (released) {
      return
    }

    released = true
    try {
      reader.releaseLock()
    } catch {}
  }

  const cancelReader = (reason: unknown) => {
    try {
      void reader
        .cancel(reason)
        .catch(() => {})
        .finally(releaseLock)
    } catch {
      releaseLock()
    }
  }

  const stream = new Readable({
    async read() {
      if (reading) {
        return
      }

      reading = true
      try {
        while (!stream.destroyed) {
          const { done, value } = await reader.read()
          if (stream.destroyed) {
            return
          }

          if (done) {
            releaseLock()
            const abortedSignal = abortSignals.find((s) => s?.aborted)
            if (abortedSignal) {
              // defensively handle abort during stream end
              stream.destroy(
                normalizeStreamError(getAbortReason(abortedSignal, new Error('aborted')))
              )
              return
            }

            stream.push(null)
            return
          }

          if (!stream.push(value)) {
            return
          }
        }
      } catch (error) {
        stream.destroy(normalizeStreamError(error))
      } finally {
        reading = false
      }
    },
    destroy(error, callback) {
      if (!released) {
        cancelReader(error)
      }

      callback(error)
    },
  })

  return stream
}

async function cancelResponseBody(response: Response) {
  try {
    await response.body?.cancel()
  } catch {}
}

function normalizeStreamError(error: unknown) {
  if (isRetryResumeUnsupportedError(error)) {
    return new Error(formatRequestErrorMessage(error), { cause: error })
  }

  return error instanceof Error ? error : new Error(String(error))
}

function parseLastModifiedHeader(value: string | undefined) {
  if (!value) {
    return undefined
  }

  const lastModified = new Date(value)
  return Number.isNaN(lastModified.getTime()) ? undefined : lastModified
}

function getAbortReason(signal: AbortSignal, fallback: unknown) {
  return signal.reason === undefined ? fallback : signal.reason
}

function buildTotalTimeoutError() {
  return new Error(`imgproxy total request timeout of ${IMGPROXY_TOTAL_TIMEOUT_MS}ms exceeded`)
}

function imageRendererResponseHeaders(headers: Headers) {
  const result: Record<string, string | undefined> = {}

  IMAGE_RENDERER_RESPONSE_HEADERS.forEach((name) => {
    result[name] = headers.get(name) ?? undefined
  })

  return result
}

function logInvalidContentLength(request: FastifyRequest, value: string) {
  logSchema.warning(request.log, 'imgproxy returned invalid content-length', {
    type: 'imgproxy',
    tenantId: request.tenantId,
    project: request.tenantId,
    reqId: request.id,
    sbReqId: request.sbReqId,
    error: new Error(`Invalid content-length header value: ${value}`),
  })
}

function formatRequestErrorMessage(error: unknown) {
  if (isHeadersTimeoutError(error)) {
    return `imgproxy headers timeout of ${IMGPROXY_REQUEST_TIMEOUT_MS}ms exceeded`
  }

  if (isBodyTimeoutError(error)) {
    return `imgproxy body timeout of ${IMGPROXY_REQUEST_TIMEOUT_MS}ms exceeded`
  }

  if (isRetryResumeUnsupportedError(error)) {
    return 'imgproxy connection dropped mid-response: retry resume is not supported'
  }

  if (error instanceof Error) {
    return error.message
  }

  if (typeof error === 'string') {
    return error
  }

  if (error == null) {
    return 'Unknown request error'
  }

  return String(error)
}

function isHeadersTimeoutError(error: unknown): boolean {
  return errorHasCode(error, 'UND_ERR_HEADERS_TIMEOUT')
}

function isBodyTimeoutError(error: unknown): boolean {
  return errorHasCode(error, 'UND_ERR_BODY_TIMEOUT')
}

function isRetryResumeUnsupportedError(error: unknown): boolean {
  return errorHasCode(error, 'UND_ERR_REQ_RETRY')
}

function errorHasCode(error: unknown, code: string): boolean {
  const seen = new WeakSet<object>()
  const stack: unknown[] = [error]
  let visited = 0

  while (stack.length > 0 && visited < 10_000) {
    const current = stack.pop()
    if (!current || typeof current !== 'object' || seen.has(current)) {
      continue
    }

    seen.add(current)
    visited += 1

    if (readErrorProperty(current, 'code') === code) {
      return true
    }

    stack.push(readErrorProperty(current, 'cause'), readErrorProperty(current, 'originalError'))
  }

  return false
}

function readErrorProperty(error: object, property: 'cause' | 'code' | 'originalError') {
  try {
    return (error as Record<typeof property, unknown>)[property]
  } catch {
    return undefined
  }
}

const clamp = (num: number, min: number, max: number) => Math.min(Math.max(num, min), max)
