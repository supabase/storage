import { ERRORS } from '@internal/errors'
import { FastifyRequest } from 'fastify'
import { pipeline, Readable, Stream, Transform } from 'stream'
import type { ReadableStream as NodeReadableStream } from 'stream/web'
import { Agent } from 'undici'
import { getConfig } from '../../config'
import { ObjectMetadata, StorageBackendAdapter } from '../backend'
import { Renderer, RenderOptions } from './renderer'

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
  imgProxyHttpMaxSockets,
  imgProxyHttpKeepAlive,
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
const MAX_RETRIES = 5
const RETRYABLE_STATUS_CODES = new Set([429, 500])

const dispatcher =
  imgProxyHttpMaxSockets > 0
    ? new Agent({
        connections: imgProxyHttpMaxSockets,
        keepAliveTimeout: imgProxyHttpKeepAlive * 1000,
        keepAliveMaxTimeout: imgProxyHttpKeepAlive * 1000,
      })
    : undefined

interface TransformLimits {
  maxResolution?: number
}

interface ImageRendererRequestOptions {
  signal?: AbortSignal
  headers?: Record<string, string | string[] | undefined>
}

interface ImageRendererResponse {
  data?: Stream
  status: number
  headers: Record<string, string | undefined>
}

interface ImageRendererClient {
  get(url: string, options?: ImageRendererRequestOptions): Promise<ImageRendererResponse>
}

class ImageRendererRequestError extends Error {
  constructor(
    message: string,
    readonly response?: ImageRendererResponse
  ) {
    super(message)
    this.name = 'ImageRendererRequestError'
  }
}

const client: ImageRendererClient = {
  async get(url, options) {
    return fetchWithRetry(url, options)
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
      switch (name) {
        case 'height':
          transformOptions.height = parseInt(String(value), 10)
          break
        case 'width':
          transformOptions.width = parseInt(String(value), 10)
          break
        case 'resize':
          transformOptions.resize = value as TransformOptions['resize']
          break
        case 'format':
          transformOptions.format = value as TransformOptions['format']
          break
        case 'quality':
          transformOptions.quality = parseInt(String(value), 10)
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

      const contentLength = parseInt(response.headers['content-length'] || '', 10)
      const lastModified = response.headers['last-modified']
        ? new Date(response.headers['last-modified'])
        : undefined

      return {
        body: response.data,
        transformations,
        metadata: {
          httpStatusCode: response.status,
          size: contentLength,
          contentLength,
          lastModified,
          eTag: headObj.eTag,
          cacheControl: headObj.cacheControl,
          mimetype: response.headers['content-type'],
        } as ObjectMetadata,
      }
    } catch (e) {
      if (e instanceof ImageRendererRequestError) {
        const error = await this.handleRequestError(e)
        throw error.withMetadata({
          transformations,
        })
      }

      throw e
    }
  }

  protected async handleRequestError(error: ImageRendererRequestError) {
    const stream = error.response?.data as Stream
    if (!stream) {
      return ERRORS.InternalError(undefined, error.message)
    }

    let errorResponse: string
    try {
      errorResponse = await new Promise<string>((resolve, reject) => {
        let errorBuffer = ''

        function cleanup() {
          stream.off('data', onData)
          stream.off('end', onEnd)
          stream.off('error', onError)
        }

        function onData(data: Buffer | Uint8Array | string) {
          errorBuffer += typeof data === 'string' ? data : Buffer.from(data).toString('utf8')
        }

        function onEnd() {
          cleanup()
          resolve(errorBuffer)
        }

        function onError(err: Error) {
          cleanup()
          reject(err)
        }

        stream.on('data', onData)
        stream.on('end', onEnd)
        stream.on('error', onError)
      })
    } catch (e) {
      return ERRORS.InternalError(e instanceof Error ? e : undefined, formatRequestErrorMessage(e))
    }

    const statusCode = error.response?.status || 500
    return ERRORS.ImageProcessingError(statusCode, errorResponse || error.message)
  }
}

async function fetchWithRetry(
  url: string,
  options: ImageRendererRequestOptions = {}
): Promise<ImageRendererResponse> {
  const requestUrl = resolveImgProxyUrl(url)

  for (let retryCount = 0; ; retryCount++) {
    let response: Response

    const controller = new AbortController()
    const timeoutId = setTimeout(() => {
      controller.abort(new DOMException('The operation timed out', 'TimeoutError'))
    }, IMGPROXY_REQUEST_TIMEOUT_MS)

    const onCallerAbort = () => {
      controller.abort(options.signal?.reason)
    }
    const cleanupCallerAbort = () => {
      options.signal?.removeEventListener('abort', onCallerAbort)
    }

    if (options.signal?.aborted) {
      onCallerAbort()
    } else if (options.signal) {
      options.signal.addEventListener('abort', onCallerAbort, { once: true })
    }

    try {
      response = await fetch(requestUrl, {
        ...createRequestInit(options),
        signal: controller.signal,
      })
    } catch (e) {
      clearTimeout(timeoutId)
      cleanupCallerAbort()

      const callerAbortReason = getCallerAbortReason(options.signal)
      if (callerAbortReason) {
        throw callerAbortReason
      }

      throw new ImageRendererRequestError(formatRequestErrorMessage(e))
    }

    // Keep caller abort wired after headers so undici can tear down the body stream.
    clearTimeout(timeoutId)

    if (response.ok) {
      return toImageRendererResponse(response, {
        idleTimeoutMs: IMGPROXY_REQUEST_TIMEOUT_MS,
        controller,
        onDone: cleanupCallerAbort,
      })
    }

    if (retryCount < MAX_RETRIES && RETRYABLE_STATUS_CODES.has(response.status)) {
      await response.body?.cancel().catch(() => {})
      cleanupCallerAbort()
      try {
        await delay(getRetryDelay(retryCount + 1, response.status), options.signal)
      } catch (e) {
        const callerAbortReason = getCallerAbortReason(options.signal)
        if (callerAbortReason) {
          throw callerAbortReason
        }

        throw new ImageRendererRequestError(formatRequestErrorMessage(e))
      }
      continue
    }

    throw new ImageRendererRequestError(
      `Request failed with status code ${response.status}`,
      toImageRendererResponse(response, {
        idleTimeoutMs: IMGPROXY_REQUEST_TIMEOUT_MS,
        controller,
        onDone: cleanupCallerAbort,
      })
    )
  }
}

function createRequestInit(options: ImageRendererRequestOptions) {
  const requestInit: RequestInit & { dispatcher?: Agent } = {
    method: 'GET',
    headers: createHeaders(options.headers),
  }

  if (dispatcher) {
    requestInit.dispatcher = dispatcher
  }

  return requestInit
}

function createHeaders(headers?: ImageRendererRequestOptions['headers']) {
  const result = new Headers()

  for (const [name, value] of Object.entries(headers || {})) {
    if (typeof value === 'string') {
      result.set(name, value)
    } else if (Array.isArray(value)) {
      result.set(name, value.join(', '))
    }
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

interface BodyIdleGuard {
  idleTimeoutMs: number
  controller: AbortController
  onDone?: () => void
}

function toImageRendererResponse(
  response: Response,
  bodyIdleGuard?: BodyIdleGuard
): ImageRendererResponse {
  let data: Stream
  if (response.body) {
    const source = Readable.fromWeb(response.body as unknown as NodeReadableStream<Uint8Array>)
    data = bodyIdleGuard
      ? wrapWithBodyIdleTimeout(
          source,
          bodyIdleGuard.idleTimeoutMs,
          bodyIdleGuard.controller,
          bodyIdleGuard.onDone
        )
      : source
  } else {
    bodyIdleGuard?.onDone?.()
    data = Readable.from([])
  }

  return {
    data,
    status: response.status,
    headers: responseHeaders(response.headers),
  }
}

// Guard against imgproxy sending headers and then stalling the body stream.
function wrapWithBodyIdleTimeout(
  source: Readable,
  idleTimeoutMs: number,
  controller: AbortController,
  onDone?: () => void
): Readable {
  let timer: NodeJS.Timeout | undefined

  const relay = new Transform({
    transform(chunk, _encoding, callback) {
      armTimer()
      callback(null, chunk)
    },
  })

  function armTimer() {
    if (timer !== undefined) {
      clearTimeout(timer)
    }
    timer = setTimeout(() => {
      timer = undefined
      const err = new DOMException(
        `idle timeout of ${idleTimeoutMs}ms exceeded while reading imgproxy response body`,
        'TimeoutError'
      )
      controller.abort(err)
      relay.destroy(err)
    }, idleTimeoutMs)
  }

  function disarmTimer() {
    if (timer !== undefined) {
      clearTimeout(timer)
      timer = undefined
    }
  }

  armTimer()
  pipeline(source, relay, () => {
    disarmTimer()
    onDone?.()
  })

  return relay
}

function responseHeaders(headers: Headers) {
  const result: Record<string, string> = {}

  headers.forEach((value, name) => {
    result[name.toLowerCase()] = value
  })

  return result
}

function getRetryDelay(retryCount: number, statusCode: number) {
  return retryCount * (statusCode === 500 ? 150 : 50)
}

async function delay(ms: number, signal?: AbortSignal) {
  if (!ms) {
    return
  }

  await new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason ?? new DOMException('The operation was aborted', 'AbortError'))
      return
    }

    const timeout = setTimeout(() => {
      cleanup()
      resolve()
    }, ms)

    function cleanup() {
      signal?.removeEventListener('abort', abort)
    }

    function abort() {
      cleanup()
      clearTimeout(timeout)
      reject(signal?.reason ?? new DOMException('The operation was aborted', 'AbortError'))
    }

    signal?.addEventListener('abort', abort, { once: true })
  })
}

function formatRequestErrorMessage(error: unknown) {
  if (error instanceof DOMException && error.name === 'TimeoutError') {
    return `timeout of ${IMGPROXY_REQUEST_TIMEOUT_MS}ms exceeded`
  }

  if (error instanceof Error) {
    return error.message
  }

  return String(error)
}

function getCallerAbortReason(signal?: AbortSignal) {
  if (!signal?.aborted) {
    return undefined
  }

  return signal.reason ?? new DOMException('The operation was aborted', 'AbortError')
}

const clamp = (num: number, min: number, max: number) => Math.min(Math.max(num, min), max)
