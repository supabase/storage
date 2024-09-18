import dotenv from 'dotenv'
import fs from 'fs/promises'
import { getConfig } from '../config'
import app from '../app'
import { S3Backend } from '../storage/backend'
import path from 'path'
import { ImageRenderer } from '../storage/renderer'
import axios from 'axios'
import { useMockObject } from './common'

dotenv.config({ path: '.env.test' })
const { imgProxyURL } = getConfig()

describe('image rendering routes', () => {
  beforeAll(async () => {
    await fs.mkdir(path.join(__dirname, '..', '..', 'data'), { recursive: true })
    await fs.copyFile(
      path.resolve(__dirname, 'assets', 'sadcat.jpg'),
      path.join(__dirname, '..', '..', 'data', 'sadcat.jpg')
    )
  })

  useMockObject()

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('will render an authenticated image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await app().inject({
      method: 'GET',
      url: '/render/image/authenticated/bucket2/authenticated/casestudy.png?width=100&height=100',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      '/public/height:100/width:100/resizing_type:fill/plain/local:///data/sadcat.jpg',
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will render a public image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await app().inject({
      method: 'GET',
      url: '/render/image/public/public-bucket-2/favicon.ico?width=100&height=100',
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      '/public/height:100/width:100/resizing_type:fill/plain/local:///data/sadcat.jpg',
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will render a transformed image providing a signed url', async () => {
    const signURLResponse = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/casestudy.png',
      payload: {
        expiresIn: 60000,
        transform: {
          width: 100,
          height: 100,
          resize: 'contain',
        },
      },
      headers: {
        authorization: `Bearer ${process.env.SERVICE_KEY}`,
      },
    })

    const signedURLBody = signURLResponse.json<{ signedURL: string }>()

    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await app().inject({
      method: 'GET',
      url: signedURLBody.signedURL,
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      '/public/height:100/width:100/resizing_type:fit/plain/local:///data/sadcat.jpg',
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })
})
