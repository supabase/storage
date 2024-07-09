import { AssetResponse, Renderer, RenderOptions } from './renderer'
import { FastifyReply, FastifyRequest } from 'fastify'
import { ObjectMetadata } from '../backend'
import { ImageRenderer, TransformOptions } from './image'
import { ERRORS } from '@internal/errors'

/**
 * HeadRenderer
 * is a special renderer that only outputs metadata information with an empty content
 */
export class HeadRenderer extends Renderer {
  async getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse> {
    const { object } = options

    if (!object) {
      throw ERRORS.NoSuchKey(`${options.bucket}/${options.key}/${options.version}`)
    }

    const metadata = object.metadata ? { ...object.metadata } : {}
    if (metadata.lastModified) {
      metadata.lastModified = new Date(metadata.lastModified as string)
    }

    return {
      metadata: metadata as ObjectMetadata,
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
