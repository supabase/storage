import { validateXRobotsTag } from '@storage/validators/x-robots-tag'
import { FastifyReply, FastifyRequest } from 'fastify'
import { Readable } from 'stream'
import { getConfig } from '../../config'
import { ObjectMetadata } from '../backend'
import { Obj } from '../schemas'

export interface RenderOptions {
  bucket: string
  key: string
  version: string | undefined
  download?: string
  expires?: string
  xRobotsTag?: string
  object?: Obj
  signal?: AbortSignal
}

export interface AssetResponse {
  body?: Readable | ReadableStream<unknown> | Blob | Buffer | Record<string, unknown>
  metadata: AssetMetadata
  transformations?: string[]
}

export type AssetMetadata = Omit<
  ObjectMetadata,
  'cacheControl' | 'contentLength' | 'eTag' | 'mimetype' | 'size'
> & {
  cacheControl?: string
  contentLength?: number
  eTag?: string
  mimetype?: string
  size?: number
}

type HttpMetadataError = {
  $metadata?: {
    httpStatusCode?: number
  }
}

const { requestEtagHeaders, responseSMaxAge } = getConfig()

/**
 * Renderer
 * a generic renderer that respond to a request with an asset content
 * and all the important headers
 */
export abstract class Renderer {
  protected sMaxAge = responseSMaxAge

  abstract getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse>

  /**
   * Renders a specific asset applying all the important headers
   * @param request
   * @param response
   * @param options
   */
  async render(request: FastifyRequest, response: FastifyReply, options: RenderOptions) {
    try {
      if (options.signal?.aborted) {
        return this.sendRequestAborted(response)
      }

      const data = await this.getAsset(request, options)

      if (options.signal?.aborted) {
        destroyAssetBody(data.body)
        return this.sendRequestAborted(response)
      }

      try {
        this.setHeaders(request, response, data, options)
        return response.send(data.body)
      } catch (err) {
        destroyAssetBody(data.body)
        throw err
      }
    } catch (err: unknown) {
      const metadata = getErrorMetadata(err)

      if (metadata?.httpStatusCode === 304) {
        return response.status(304).send()
      }

      if (metadata?.httpStatusCode === 404) {
        response.header('cache-control', 'no-store')
        return response.status(400).send({
          error: 'Not found',
          message: 'The resource was not found',
          statusCode: '404',
        })
      }

      if (isCallerAbort(err, options.signal)) {
        return this.sendRequestAborted(response)
      }

      throw err
    }
  }

  protected setHeaders(
    request: FastifyRequest,
    response: FastifyReply,
    data: AssetResponse,
    options: RenderOptions
  ) {
    let xRobotsTag = 'none'
    if (options.xRobotsTag) {
      try {
        // allow overriding x-robots-tag header only with valid values
        validateXRobotsTag(options.xRobotsTag)
        xRobotsTag = options.xRobotsTag
      } catch {}
    }

    response
      .status(data.metadata.httpStatusCode ?? 200)
      .header('Accept-Ranges', 'bytes')
      .header('Content-Type', normalizeContentType(data.metadata.mimetype))
      .header('ETag', data.metadata.eTag)
      .header('X-Robots-Tag', xRobotsTag)

    this.setLastModifiedHeader(response, data.metadata.lastModified)
    this.setContentLengthHeader(response, data.metadata.contentLength)

    if (options.expires) {
      response.header('Expires', options.expires)
    }
    this.handleCacheControl(request, response, data.metadata)

    if (data.metadata.contentRange) {
      response.header('Content-Range', data.metadata.contentRange)
    }

    if (data.transformations && data.transformations.length > 0) {
      response.header('X-Transformations', data.transformations.join(','))
    }

    this.handleDownload(response, options.download)
  }

  protected handleDownload(response: FastifyReply, download?: string) {
    if (typeof download !== 'undefined') {
      if (download === '') {
        response.header('Content-Disposition', 'attachment;')
      } else {
        const encodedFileName = encodeURIComponent(download)

        response.header(
          'Content-Disposition',
          `attachment; filename=${encodedFileName}; filename*=UTF-8''${encodedFileName}`
        )
      }
    }
  }

  protected handleCacheControl(
    request: FastifyRequest,
    response: FastifyReply,
    metadata: AssetMetadata
  ) {
    const etag = this.findEtagHeader(request)

    const cacheControl = [metadata.cacheControl]

    if (!etag) {
      this.setCacheControlHeader(response, cacheControl)
      return
    }

    if (this.sMaxAge > 0) {
      cacheControl.push(`s-maxage=${this.sMaxAge}`)
    }

    if (etag !== metadata.eTag) {
      cacheControl.push('stale-while-revalidate=30')
    }

    this.setCacheControlHeader(response, cacheControl)
  }

  protected setContentLengthHeader(response: FastifyReply, contentLength: number | undefined) {
    if (contentLength !== undefined) {
      response.header('Content-Length', contentLength)
    }
  }

  protected setLastModifiedHeader(response: FastifyReply, lastModified: Date | undefined) {
    if (lastModified && !Number.isNaN(lastModified.getTime())) {
      response.header('Last-Modified', lastModified.toUTCString())
    }
  }

  protected setCacheControlHeader(response: FastifyReply, values: Array<string | undefined>) {
    const cacheControl = values.filter((value) => typeof value === 'string' && value.length > 0)
    if (cacheControl.length > 0) {
      response.header('Cache-Control', cacheControl.join(', '))
    }
  }

  protected sendRequestAborted(response: FastifyReply) {
    return response.status(499).send({ error: 'Request aborted', statusCode: '499' })
  }

  protected findEtagHeader(request: FastifyRequest) {
    for (const header of requestEtagHeaders) {
      const etag = request.headers[header]
      if (etag) {
        return etag
      }
    }
  }
}

function isCallerAbort(error: unknown, signal: AbortSignal | undefined) {
  if (!signal?.aborted) {
    return false
  }

  if (signal.reason === undefined) {
    return isAbortError(error)
  }

  if (error === signal.reason || hasErrorCause(error, signal.reason)) {
    return true
  }

  // AWS SDK via @smithy/node-http-handler creates a fresh AbortError without
  // preserving the original reason. Once our signal is aborted, treat that as
  // caller-driven to surface 499 instead of 500.
  return isAbortError(error)
}

function isAbortError(error: unknown) {
  if (!error || typeof error !== 'object') {
    return false
  }

  const name = readErrorProperty(error, 'name')
  return (
    name === 'AbortError' &&
    (error instanceof Error ||
      (typeof DOMException !== 'undefined' && error instanceof DOMException))
  )
}

function hasErrorCause(error: unknown, cause: unknown): boolean {
  if (cause === undefined) {
    return false
  }

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

    const nestedCause = readErrorProperty(current, 'cause')
    const originalError = readErrorProperty(current, 'originalError')
    if (nestedCause === cause || originalError === cause) {
      return true
    }

    stack.push(nestedCause, originalError)
  }

  return false
}

function readErrorProperty(error: object, property: 'cause' | 'name' | 'originalError') {
  try {
    return (error as Record<typeof property, unknown>)[property]
  } catch {
    return undefined
  }
}

function destroyAssetBody(body: AssetResponse['body']) {
  if (!body || typeof body !== 'object') {
    return
  }

  if (body instanceof Readable) {
    body.destroy()
    return
  }

  if (isReadableStream(body)) {
    const result = body.cancel()
    void result.catch(() => {})
  }
}

function isReadableStream(body: object): body is ReadableStream {
  return typeof ReadableStream !== 'undefined' && body instanceof ReadableStream
}

function normalizeContentType(contentType: string | undefined): string | undefined {
  if (contentType?.includes('text/html')) {
    return 'text/plain'
  }
  return contentType
}

function getErrorMetadata(error: unknown): HttpMetadataError['$metadata'] {
  if (!error || typeof error !== 'object') {
    return undefined
  }

  return (error as HttpMetadataError).$metadata
}
