import { FastifyRequest } from 'fastify'
import { Renderer, RenderOptions } from './renderer'
import { Storage } from '../storage'

/**
 * AssetRenderer
 * renders an asset from a backend adapter
 */
export class AssetRenderer extends Renderer {
  constructor(private readonly storage: Storage) {
    super()
  }

  getAsset(request: FastifyRequest, options: RenderOptions) {
    return this.storage.backend.getObject(options.bucket, options.key, options.version, {
      ifModifiedSince: request.headers['if-modified-since'],
      ifNoneMatch: request.headers['if-none-match'],
      range: request.headers.range,
    })
  }
}
