import { FastifyRequest } from 'fastify'
import { StorageBackendAdapter } from '../backend'
import { Renderer, RenderOptions } from './renderer'

/**
 * AssetRenderer
 * renders an asset from a backend adapter
 */
export class AssetRenderer extends Renderer {
  constructor(private readonly backend: StorageBackendAdapter) {
    super()
  }

  getAsset(request: FastifyRequest, options: RenderOptions) {
    return this.backend.getObject(
      options.bucket,
      options.key,
      options.version,
      {
        ifMatch: request.headers['if-match'],
        ifModifiedSince: request.headers['if-modified-since'],
        ifNoneMatch: request.headers['if-none-match'],
        ifUnmodifiedSince: request.headers['if-unmodified-since'],
        range: request.headers.range,
      },
      options.signal
    )
  }
}
