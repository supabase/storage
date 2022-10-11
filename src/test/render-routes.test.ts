import dotenv from 'dotenv'
import fs from 'fs/promises'
import { getConfig } from '../utils/config'
import app from '../app'
import { S3Backend } from '../backend/s3'
import path from 'path'
import { Imgproxy } from '../renderer/imgproxy'
import axios from 'axios'

dotenv.config({ path: '.env.test' })
const ENV = process.env
const { imgProxyURL } = getConfig()

describe('image rendering routes', () => {
  beforeAll(async () => {
    await fs.mkdir(path.join(__dirname, '..', '..', 'data'), { recursive: true })
    await fs.copyFile(
      path.resolve(__dirname, 'assets', 'sadcat.jpg'),
      path.join(__dirname, '..', '..', 'data', 'sadcat.jpg')
    )
  })
  beforeEach(() => {
    process.env = { ...ENV }

    jest.spyOn(S3Backend.prototype, 'getObject').mockResolvedValue({
      metadata: {
        httpStatusCode: 200,
        size: 3746,
        mimetype: 'image/png',
        lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
        eTag: 'abc',
      },
      body: Buffer.from(''),
    })

    jest.spyOn(S3Backend.prototype, 'uploadObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
    })

    jest.spyOn(S3Backend.prototype, 'copyObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
    })

    jest.spyOn(S3Backend.prototype, 'deleteObject').mockResolvedValue({})

    jest.spyOn(S3Backend.prototype, 'deleteObjects').mockResolvedValue({})

    jest.spyOn(S3Backend.prototype, 'headObject').mockResolvedValue({
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
    })

    jest.spyOn(S3Backend.prototype, 'privateAssetUrl').mockResolvedValue('local:///data/sadcat.jpg')
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('will render an authenticated image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(Imgproxy.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await app().inject({
      method: 'GET',
      url: '/render/authenticated/bucket2/authenticated/casestudy.png?width=100&height=100',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      '/public/height:100/width:100/resizing_type:fill/plain/local:///data/sadcat.jpg',
      { responseType: 'stream' }
    )
  })

  it('will render a public image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(Imgproxy.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await app().inject({
      method: 'GET',
      url: '/render/public/public-bucket-2/favicon.ico?width=100&height=100',
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      '/public/height:100/width:100/resizing_type:fill/plain/local:///data/sadcat.jpg',
      { responseType: 'stream' }
    )
  })
})
