import { Obj } from '@storage/schemas'
import { HeadRenderer } from './head'
import { FastifyRequest, FastifyReply } from 'fastify'
import { AssetResponse, RenderOptions } from './renderer'
import { ImageRenderer, TransformOptions } from '@storage/renderer/image'

/**
 * HeadRenderer
 * is a special renderer that only outputs metadata information with an empty content
 */
export class InfoRenderer extends HeadRenderer {
  async getAsset(request: FastifyRequest, options: RenderOptions): Promise<AssetResponse> {
    const headAsset = await super.getAsset(request, options)

    const obj = options.object as Obj

    return {
      ...headAsset,
      transformations: ImageRenderer.applyTransformation(request.query as TransformOptions),
      body: {
        id: obj.id,
        name: obj.name,
        version: obj.version,
        bucket_id: obj.bucket_id,
        size: obj.metadata?.size ?? null,
        content_type: obj.metadata?.mimetype ?? null,
        cache_control: obj.metadata?.cacheControl ?? null,
        etag: obj.metadata?.eTag ?? null,
        metadata: obj.user_metadata,
        last_modified: obj.updated_at,
        created_at: obj.created_at,
      },
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
      .header('Content-Type', 'application/json')
      .header('ETag', data.metadata.eTag)
      .header('Content-Length', data.metadata.contentLength)
      .header('Last-Modified', data.metadata.lastModified?.toUTCString())
      .header('Cache-Control', data.metadata.cacheControl)

    if (data.transformations && data.transformations.length > 0) {
      response.header('X-Transformations', data.transformations.join(','))
    }
  }
}
