import { AssetResponse, Renderer, RenderOptions } from './renderer'
import { FastifyReply, FastifyRequest } from 'fastify'
import { ImageRenderer, TransformOptions } from './image'
import { Storage } from '../storage'
import { ERRORS } from '../errors'
import { ObjMetadata } from '../schemas'

/**
 * HeadRenderer
 * is a special renderer that only outputs metadata information with an empty content
 */
export class HeadRenderer extends Renderer {
  constructor(private readonly storage: Storage) {
    super()
  }

  async getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse> {
    const { metadata } = await this.storage.from(options.bucket).findObject(options.key, 'metadata')

    if (!metadata) {
      throw ERRORS.MetadataNotFound()
    }

    return {
      metadata,
      transformations: ImageRenderer.applyTransformation(request.query as TransformOptions),
    }
  }

  protected handleCacheControl(
    request: FastifyRequest<any>,
    response: FastifyReply<any>,
    metadata: ObjMetadata
  ) {
    const etag = this.findEtagHeader(request)
    const cacheControl = metadata.cacheControl ?? 'no-cache'

    const newCacheControl: string[] = [cacheControl]

    if (!etag || ['no-store', 'no-cache'].includes(cacheControl)) {
      response.header('Cache-Control', newCacheControl.join(', '))
      return
    }

    if (etag !== metadata.eTag) {
      newCacheControl.push('must-revalidate')
    } else if (this.sMaxAge > 0) {
      newCacheControl.push(`s-maxage=${this.sMaxAge}`)
    }

    response.header('Cache-Control', newCacheControl.join(', '))
  }
}
