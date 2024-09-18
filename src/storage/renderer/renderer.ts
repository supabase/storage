import { FastifyReply, FastifyRequest } from 'fastify'
import { ObjectMetadata } from '../backend'
import { Readable } from 'stream'
import { getConfig } from '../../config'
import { Obj } from '../schemas'

export interface RenderOptions {
  bucket: string
  key: string
  version: string | undefined
  download?: string
  expires?: string
  object?: Obj
  signal?: AbortSignal
}

export interface AssetResponse {
  body?: Readable | ReadableStream<any> | Blob | Buffer | Record<any, any>
  metadata: ObjectMetadata
  transformations?: string[]
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
  async render(request: FastifyRequest<any>, response: FastifyReply<any>, options: RenderOptions) {
    try {
      if (options.signal?.aborted) {
        return response.send({ error: 'Request aborted', statusCode: '499' })
      }

      const data = await this.getAsset(request, options)

      this.setHeaders(request, response, data, options)

      return response.send(data.body)
    } catch (err: any) {
      if (err.$metadata?.httpStatusCode === 304) {
        return response.status(304).send()
      }

      if (err.$metadata?.httpStatusCode === 404) {
        response.header('cache-control', 'no-store')
        return response.status(400).send({
          error: 'Not found',
          message: 'The resource was not found',
          statusCode: '404',
        })
      }

      throw err
    }
  }

  protected setHeaders(
    request: FastifyRequest<any>,
    response: FastifyReply<any>,
    data: AssetResponse,
    options: RenderOptions
  ) {
    response
      .status(data.metadata.httpStatusCode ?? 200)
      .header('Accept-Ranges', 'bytes')
      .header('Content-Type', normalizeContentType(data.metadata.mimetype))
      .header('ETag', data.metadata.eTag)
      .header('Content-Length', data.metadata.contentLength)
      .header('Last-Modified', data.metadata.lastModified?.toUTCString())

    if (options.expires) {
      response.header('Expires', options.expires)
    } else {
      this.handleCacheControl(request, response, data.metadata)
    }

    if (data.metadata.contentRange) {
      response.header('Content-Range', data.metadata.contentRange)
    }

    if (data.transformations && data.transformations.length > 0) {
      response.header('X-Transformations', data.transformations.join(','))
    }

    this.handleDownload(response, options.download)
  }

  protected handleDownload(response: FastifyReply<any>, download?: string) {
    if (typeof download !== 'undefined') {
      if (download === '') {
        response.header('Content-Disposition', 'attachment;')
      } else {
        const encodedFileName = encodeURIComponent(download)

        response.header(
          'Content-Disposition',
          `attachment; filename=${encodedFileName}; filename*=UTF-8''${encodedFileName};`
        )
      }
    }
  }

  protected handleCacheControl(
    request: FastifyRequest<any>,
    response: FastifyReply<any>,
    metadata: ObjectMetadata
  ) {
    const etag = this.findEtagHeader(request)

    const cacheControl = [metadata.cacheControl]

    if (!etag) {
      response.header('Cache-Control', cacheControl.join(', '))
      return
    }

    if (this.sMaxAge > 0) {
      cacheControl.push(`s-maxage=${this.sMaxAge}`)
    }

    if (etag !== metadata.eTag) {
      cacheControl.push('stale-while-revalidate=30')
    }

    response.header('Cache-Control', cacheControl.join(', '))
  }

  protected findEtagHeader(request: FastifyRequest<any>) {
    for (const header of requestEtagHeaders) {
      const etag = request.headers[header]
      if (etag) {
        return etag
      }
    }
  }
}

function normalizeContentType(contentType: string | undefined): string | undefined {
  if (contentType?.includes('text/html')) {
    return 'text/plain'
  }
  return contentType
}
