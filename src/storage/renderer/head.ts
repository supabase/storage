import { AssetResponse, Renderer, RenderOptions } from './renderer'
import { FastifyRequest } from 'fastify'
import { GenericStorageBackend } from '../backend'
import { ImageRenderer, TransformOptions } from './image'

export class HeadRenderer extends Renderer {
  constructor(private readonly backend: GenericStorageBackend) {
    super()
  }

  async getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse> {
    const metadata = await this.backend.headObject(options.bucket, options.key)

    return {
      metadata,
      transformations: ImageRenderer.applyTransformation(request.query as TransformOptions),
    }
  }
}
