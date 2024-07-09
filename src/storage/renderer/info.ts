import { HeadRenderer } from './head'
import { FastifyRequest } from 'fastify'
import { AssetResponse, RenderOptions } from './renderer'
import { Obj } from '@storage/schemas'
import { FastifyReply } from 'fastify/types/reply'

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
      body: obj,
    }
  }

  protected setHeaders(
    request: FastifyRequest<any>,
    response: FastifyReply<any>,
    data: AssetResponse,
    options: RenderOptions
  ) {
    // no-op
  }
}
