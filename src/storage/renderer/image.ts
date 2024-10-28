import { ObjectMetadata, StorageBackendAdapter } from '../backend'
import axios, { Axios, AxiosError } from 'axios'
import { getConfig } from '../../config'
import { FastifyRequest } from 'fastify'
import { Renderer, RenderOptions } from './renderer'
import axiosRetry from 'axios-retry'
import { ERRORS } from '@internal/errors'
import { Stream } from 'stream'
import Agent from 'agentkeepalive'

/**
 * All the transformations options available
 */
export interface TransformOptions {
  width?: number
  height?: number
  resize?: 'cover' | 'contain' | 'fill'
  format?: 'origin' | 'avif'
  quality?: number
}

const {
  imgLimits,
  imgProxyHttpMaxSockets,
  imgProxyHttpKeepAlive,
  imgProxyURL,
  imgProxyRequestTimeout,
} = getConfig()

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
  timeout: imgProxyRequestTimeout * 1000,
  httpAgent:
    imgProxyHttpMaxSockets > 0
      ? new Agent({
          maxSockets: imgProxyHttpMaxSockets,
          freeSocketTimeout: 2 * 1000,
          keepAlive: true,
          timeout: imgProxyHttpKeepAlive * 1000,
        })
      : undefined,
})

axiosRetry(client, {
  retries: 5,
  shouldResetTimeout: true,
  retryDelay: (retryCount, error) => {
    let exponentialTime = 50

    if (error.response?.status === 500) {
      exponentialTime = 150
    }
    return retryCount * exponentialTime
  },
  retryCondition: async (err) => {
    return [429, 500].includes(err.response?.status || 0)
  },
})

interface TransformLimits {
  maxResolution?: number
}

/**
 * ImageRenderer
 * renders an image by applying transformations
 *
 * Interacts with an imgproxy backend for the actual transformation
 */
export class ImageRenderer extends Renderer {
  private readonly client: Axios
  private transformOptions?: TransformOptions
  private limits?: TransformLimits

  constructor(private readonly backend: StorageBackendAdapter) {
    super()
    this.client = client
  }

  /**
   * Applies whitelisted transformations with specific limits applied
   * @param options
   * @param keepOriginal
   */
  static applyTransformation(options: TransformOptions, keepOriginal?: boolean): string[] {
    const segments = []

    if (options.height) {
      segments.push(`height:${clamp(options.height, LIMITS.height.min, LIMITS.height.max)}`)
    }

    if (options.width) {
      segments.push(`width:${clamp(options.width, LIMITS.width.min, LIMITS.width.max)}`)
    }

    if (options.width || options.height) {
      if (keepOriginal) {
        segments.push(`resize:${options.resize}`)
      } else {
        segments.push(`resizing_type:${this.formatResizeType(options.resize)}`)
      }
    }

    if (options.quality) {
      segments.push(`quality:${options.quality}`)
    }

    if (options.format && options.format !== 'origin') {
      segments.push(`format:${options.format}`)
    }

    return segments
  }

  static applyTransformationLimits(limits: TransformLimits) {
    const transforms: string[] = []
    if (typeof limits?.maxResolution === 'number') {
      transforms.push(`max_src_resolution:${limits.maxResolution}`)
    }

    return transforms
  }

  protected static formatResizeType(resize: TransformOptions['resize']) {
    const defaultResize = 'fill'

    switch (resize) {
      case 'cover':
        return defaultResize
      case 'contain':
        return 'fit'
      case 'fill':
        return 'force'
      default:
        return defaultResize
    }
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

  setLimits(limits: TransformLimits) {
    this.limits = limits
    return this
  }

  setTransformationsFromString(transformations: string) {
    const params = transformations.split(',')

    this.transformOptions = params.reduce((all, param) => {
      const [name, value] = param.split(':') as [keyof TransformOptions, any]
      switch (name) {
        case 'height':
          all.height = parseInt(value, 10)
          break
        case 'width':
          all.width = parseInt(value, 10)
          break
        case 'resize':
          all.resize = value
          break
        case 'format':
          all.format = value
          break
        case 'quality':
          all.quality = parseInt(value, 10)
          break
      }
      return all
    }, {} as TransformOptions)

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
    const [privateURL, headObj] = await Promise.all([
      this.backend.privateAssetUrl(options.bucket, options.key, options.version),
      this.backend.headObject(options.bucket, options.key, options.version),
    ])
    const transformations = ImageRenderer.applyTransformation(this.transformOptions || {})
    const transformLimits = ImageRenderer.applyTransformationLimits(this.limits || {})

    const url = [
      '/public',
      ...transformations,
      ...transformLimits,
      'plain',
      privateURL.startsWith('local://') ? privateURL : encodeURIComponent(privateURL),
    ]

    try {
      const acceptHeader =
        this.transformOptions?.format !== 'origin' ? request.headers['accept'] : undefined

      const response = await this.getClient().get(url.join('/'), {
        responseType: 'stream',
        signal: options.signal,
        headers: acceptHeader
          ? {
              accept: acceptHeader,
            }
          : undefined,
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
          eTag: headObj.eTag,
          cacheControl: headObj.cacheControl,
          mimetype: response.headers['content-type'],
        } as ObjectMetadata,
      }
    } catch (e) {
      if (e instanceof AxiosError) {
        const error = await this.handleRequestError(e)
        throw error.withMetadata({
          transformations,
        })
      }

      throw e
    }
  }

  protected async handleRequestError(error: AxiosError) {
    const stream = error.response?.data as Stream
    if (!stream) {
      throw ERRORS.InternalError(undefined, error.message)
    }

    const errorResponse = await new Promise<string>((resolve) => {
      let errorBuffer = ''

      stream.on('data', (data) => {
        errorBuffer += data
      })

      stream.on('end', () => {
        resolve(errorBuffer)
      })
    })

    const statusCode = error.response?.status || 500
    return ERRORS.ImageProcessingError(statusCode, errorResponse)
  }
}

const clamp = (num: number, min: number, max: number) => Math.min(Math.max(num, min), max)
