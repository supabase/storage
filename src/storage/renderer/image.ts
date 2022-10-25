import { StorageBackendAdapter, ObjectMetadata } from '../backend'
import axios, { Axios } from 'axios'
import { getConfig } from '../../config'
import { FastifyRequest } from 'fastify'
import { Renderer, RenderOptions } from './renderer'

/**
 * All the transformations options available
 */
export interface TransformOptions {
  width?: number
  height?: number
  resize?: 'fill' | 'fit' | 'fill-down' | 'force' | 'auto'
}

const { imgLimits, imgProxyURL } = getConfig()

const LIMITS = {
  height: {
    min: imgLimits.size.min,
    max: imgLimits.size.max,
  },
  width: {
    min: imgLimits.size.min,
    max: imgLimits.size.max,
  },
}

const client = axios.create({
  baseURL: imgProxyURL,
  timeout: 8000,
})

/**
 * ImageRenderer
 * renders an image by applying transformations
 *
 * Interacts with an imgproxy backend for the actual transformation
 */
export class ImageRenderer extends Renderer {
  private readonly client: Axios
  private transformOptions?: TransformOptions

  constructor(private readonly backend: StorageBackendAdapter) {
    super()
    this.client = client
  }

  /**
   * Get the base http client
   */
  getClient() {
    return this.client
  }

  /**
   * Set transformations parameters before calling the render method
   * @param transformations
   */
  setTransformations(transformations: TransformOptions) {
    this.transformOptions = transformations
    return this
  }

  /**
   * Fetch the transformed asset from imgproxy.
   * We use a secure signed url in order for imgproxy to download and
   * transform the image
   * @param request
   * @param options
   */
  async getAsset(request: FastifyRequest, options: RenderOptions) {
    const privateURL = await this.backend.privateAssetUrl(options.bucket, options.key)
    const transformations = ImageRenderer.applyTransformation(this.transformOptions || {})

    const url = [
      '/public',
      ...transformations,
      'plain',
      privateURL.startsWith('local://') ? privateURL : encodeURIComponent(privateURL),
    ]

    const response = await this.getClient().get(url.join('/'), {
      responseType: 'stream',
    })

    const contentLength = parseInt(response.headers['content-length'], 10)
    const lastModified = response.headers['last-modified']
      ? new Date(response.headers['last-modified'])
      : undefined

    return {
      body: response.data,
      transformations,
      metadata: {
        httpStatusCode: response.status,
        size: contentLength,
        contentLength: contentLength,
        lastModified: lastModified,
        eTag: response.headers['etag'],
        cacheControl: response.headers['cache-control'],
        mimetype: response.headers['content-type'],
      } as ObjectMetadata,
    }
  }

  /**
   * Applies whitelisted transformations with specific limits applied
   * @param options
   */
  static applyTransformation(options: TransformOptions) {
    const segments = []

    if (options.height) {
      segments.push(`height:${clamp(options.height, LIMITS.height.min, LIMITS.height.max)}`)
    }

    if (options.width) {
      segments.push(`width:${clamp(options.width, LIMITS.width.min, LIMITS.width.max)}`)
    }

    if (options.width || options.height) {
      segments.push(`resizing_type:${options.resize || 'fill'}`)
    }

    return segments
  }
}

const clamp = (num: number, min: number, max: number) => Math.min(Math.max(num, min), max)
