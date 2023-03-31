import { AssetResponse, Renderer, RenderOptions } from './renderer'
import { FastifyRequest } from 'fastify'
import { StorageBackendAdapter } from '../backend'
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
}
