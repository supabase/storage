import dotenv from 'dotenv'
import fs from 'fs/promises'
import { getConfig, JwksConfig, mergeConfig } from '../config'
import app from '../app'
import { S3Backend } from '../storage/backend'
import path from 'path'
import { ImageRenderer } from '../storage/renderer'
import axios from 'axios'
import { useMockObject } from './common'
import { generateHS512JWK, SignedToken, signJWT, verifyJWT } from '@internal/auth'
import { FastifyInstance } from 'fastify'

dotenv.config({ path: '.env.test' })
const { imgProxyURL, jwtSecret } = getConfig()
let appInstance: FastifyInstance

const projectRoot = path.join(__dirname, '..', '..')

describe('image rendering routes', () => {
  beforeAll(async () => {
    await fs.mkdir(path.join(__dirname, '..', '..', 'data'), { recursive: true })
    await fs.copyFile(
      path.resolve(__dirname, 'assets', 'sadcat.jpg'),
      path.join(__dirname, '..', '..', 'data', 'sadcat.jpg')
    )
  })

  useMockObject()

  beforeEach(() => {
    getConfig({ reload: true })
    appInstance = app()
  })

  afterEach(async () => {
    await appInstance.close()
    jest.clearAllMocks()
  })

  it('will render an authenticated image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await appInstance.inject({
      method: 'GET',
      url: '/render/image/authenticated/bucket2/authenticated/casestudy.png?width=100&height=100',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      `/public/height:100/width:100/resizing_type:fill/plain/local:///${projectRoot}/data/sadcat.jpg`,
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will render a public image applying transformations using external image processing', async () => {
    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await appInstance.inject({
      method: 'GET',
      url: '/render/image/public/public-bucket-2/favicon.ico?width=100&height=100',
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      `/public/height:100/width:100/resizing_type:fill/plain/local:///${projectRoot}/data/sadcat.jpg`,
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will render a transformed image providing a signed url', async () => {
    const assetUrl = 'bucket2/authenticated/casestudy.png'
    const signURLResponse = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/' + assetUrl,
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
    expect(signedURLBody.signedURL).toContain('?token=')

    // verify was correctly signed with jwtSecret
    const token = signedURLBody.signedURL.split('?token=').pop()!
    const jwtData = (await verifyJWT(token, jwtSecret)) as SignedToken
    expect(jwtData.url).toBe(assetUrl)

    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await appInstance.inject({
      method: 'GET',
      url: signedURLBody.signedURL,
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      `/public/height:100/width:100/resizing_type:fit/plain/local:///${projectRoot}/data/sadcat.jpg`,
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will render a transformed image providing a signed url (using url signing jwk if set)', async () => {
    const signingJwk = { ...(await generateHS512JWK()), kid: 'qwerty-09876' }
    const jwtJWKS: JwksConfig = { keys: [signingJwk], urlSigningKey: signingJwk }
    mergeConfig({ jwtJWKS })

    const assetUrl = 'bucket2/authenticated/casestudy.png'
    const signURLResponse = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/' + assetUrl,
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
    expect(signedURLBody.signedURL).toContain('?token=')

    // verify was correctly signed with url signing key (jwk)
    const token = signedURLBody.signedURL.split('?token=').pop()!
    const jwtData = (await verifyJWT(token, 'invalid-old-jwt-secret', jwtJWKS)) as SignedToken
    expect(jwtData.url).toBe(assetUrl)

    const testAxios = axios.create({ baseURL: imgProxyURL })
    jest.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testAxios)
    const axiosSpy = jest.spyOn(testAxios, 'get')

    const response = await appInstance.inject({
      method: 'GET',
      url: signedURLBody.signedURL,
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toBeCalledTimes(1)
    expect(axiosSpy).toBeCalledWith(
      `/public/height:100/width:100/resizing_type:fit/plain/local:///${projectRoot}/data/sadcat.jpg`,
      { responseType: 'stream', signal: expect.any(AbortSignal) }
    )
  })

  it('will reject malformed jwt', async () => {
    const token = 'this is not a jwt'
    const url = '/render/image/sign/bucket2/authenticated/casestudy.png?token=' + token
    const response = await appInstance.inject({ method: 'GET', url })

    expect(S3Backend.prototype.privateAssetUrl).not.toHaveBeenCalled()
    expect(response.statusCode).toBe(400)
    const body = response.json<{ error: string }>()
    expect(body.error).toBe('InvalidJWT')
  })

  it('will reject jwt with incorrect url payload', async () => {
    const token = await signJWT(
      {
        url: 'not/the/correct/url-path.png',
        transformations: 'height:100,width:100,resize:contain',
      },
      jwtSecret,
      100
    )
    const url = '/render/image/sign/bucket2/authenticated/casestudy.png?token=' + token
    const response = await appInstance.inject({ method: 'GET', url })

    expect(S3Backend.prototype.privateAssetUrl).not.toHaveBeenCalled()
    expect(response.statusCode).toBe(400)
    const body = response.json<{ error: string }>()
    expect(body.error).toBe('InvalidSignature')
  })
})
