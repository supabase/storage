import { GenericStorageBackend } from '../backend/generic'
import axios, { Axios } from 'axios'
import { getConfig } from '../utils/config'

interface TransformOptions {
  width?: number
  height?: number
  resize?: 'fill' | 'fit' | 'fill-down' | 'force' | 'auto'
}

interface ImgProxyOptions {
  url: string
}

const { imgLimits } = getConfig()

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

export class Imgproxy {
  private client: Axios

  constructor(
    private readonly backend: GenericStorageBackend,
    private readonly options: ImgProxyOptions
  ) {
    this.client = axios.create({
      baseURL: options.url,
      timeout: 8000,
    })
  }

  getClient() {
    return this.client
  }

  async transform(bucket: string, key: string, options: TransformOptions) {
    const privateURL = await this.backend.privateAssetUrl(bucket, key)
    const urlTransformation = this.applyURLTransformation(options)

    const url = [
      '/public',
      ...urlTransformation,
      'plain',
      privateURL.startsWith('local://') ? privateURL : encodeURIComponent(privateURL),
    ]

    const response = await this.getClient().get(url.join('/'), {
      responseType: 'stream',
    })

    return {
      response,
      urlTransformation,
    }
  }

  protected applyURLTransformation(options: TransformOptions) {
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
