import { FastifyRequest } from 'fastify'
import { StorageDisk } from '../disks'
import { Renderer, RenderOptions } from './renderer'

/**
 * AssetRenderer
 * renders an asset from a backend adapter
 */
export class AssetRenderer extends Renderer {
  constructor(private readonly disk: StorageDisk) {
    super()
  }

  getAsset(request: FastifyRequest, options: RenderOptions) {
    return this.disk.read({
      bucket: options.bucket,
      key: options.key,
      version: options.version,
      headers: {
        ifModifiedSince: request.headers['if-modified-since'],
        ifNoneMatch: request.headers['if-none-match'],
        range: request.headers.range,
      },
    })
  }
}
