import { AssetResponse, Renderer, RenderOptions } from './renderer'
import { FastifyReply, FastifyRequest } from 'fastify'
import { ObjectMetadata, StorageBackendAdapter } from '../backend'
import { ImageRenderer, TransformOptions } from './image'

/**
 * HeadRenderer
 * is a special renderer that only outputs metadata information with an empty content
 */
export class HeadRenderer extends Renderer {
  constructor(private readonly backend: StorageBackendAdapter) {
    super()
  }

  async getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse> {
    const metadata = await this.backend.headObject(options.bucket, options.key, options.version)

    return {
      metadata,
      transformations: ImageRenderer.applyTransformation(request.query as TransformOptions),
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

    if (etag !== metadata.eTag) {
      cacheControl.push('must-revalidate')
    } else if (this.sMaxAge > 0) {
      cacheControl.push(`s-maxage=${this.sMaxAge}`)
    }

    response.header('Cache-Control', cacheControl.join(', '))
  }
}
