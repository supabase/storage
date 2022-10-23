import { FastifyRequest } from 'fastify'
import { GenericStorageBackend } from '../backend'
import { Renderer, RenderOptions } from './renderer'

export class AssetRenderer extends Renderer {
  constructor(private readonly backend: GenericStorageBackend) {
    super()
  }

  getAsset(request: FastifyRequest, options: RenderOptions) {
    return this.backend.getObject(options.bucket, options.key, {
      ifModifiedSince: request.headers['if-modified-since'],
      ifNoneMatch: request.headers['if-none-match'],
      range: request.headers.range,
    })
  }
}
