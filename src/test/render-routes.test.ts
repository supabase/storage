import { generateHS512JWK, SignedToken, signJWT, verifyJWT } from '@internal/auth'
import dotenv from 'dotenv'
import { FastifyInstance } from 'fastify'
import fs from 'fs/promises'
import path from 'path'
import { Readable } from 'stream'
import app from '../app'
import { getConfig, JwksConfig, mergeConfig } from '../config'
import { S3Backend } from '../storage/backend'
import { ImageRenderer } from '../storage/renderer'
import { useMockObject } from './common'

dotenv.config({ path: '.env.test' })
const { jwtSecret } = getConfig()
let appInstance: FastifyInstance

const projectRoot = path.join(__dirname, '..', '..')
const renderedBodyText = 'mock-rendered-image-body'

function createMockRendererClient() {
  const bodyChunks = [Buffer.from('mock-rendered-'), Buffer.from('image-body')]
  const body = Buffer.concat(bodyChunks)
  const get = vi.fn().mockResolvedValue({
    data: Readable.from(bodyChunks),
    status: 200,
    headers: {
      'content-length': String(body.length),
      'content-type': 'image/png',
      'last-modified': 'Wed, 12 Oct 2022 11:17:02 GMT',
    },
  })
  const client: ReturnType<ImageRenderer['getClient']> = { get }

  return {
    body,
    client,
    get,
  }
}

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
    vi.restoreAllMocks()
    vi.clearAllMocks()
  })

  it('will render an authenticated image applying transformations using external image processing', async () => {
    const { client: testClient, get: getSpy } = createMockRendererClient()
    vi.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testClient)

    const response = await appInstance.inject({
      method: 'GET',
      url: '/render/image/authenticated/bucket2/authenticated/casestudy.png?width=100&height=100',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(response.body).toBe(renderedBodyText)
    expect(S3Backend.prototype.privateAssetUrl).toHaveBeenCalledTimes(1)
    expect(getSpy).toHaveBeenCalledWith(
      `/public/height:100/width:100/resizing_type:fill/plain/local:///${projectRoot}/data/sadcat.jpg`,
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
    )
  })

  it('will render a public image applying transformations using external image processing', async () => {
    const { client: testClient, get: getSpy } = createMockRendererClient()
    vi.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testClient)

    const response = await appInstance.inject({
      method: 'GET',
      url: '/render/image/public/public-bucket-2/favicon.ico?width=100&height=100',
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toHaveBeenCalledTimes(1)
    expect(getSpy).toHaveBeenCalledWith(
      `/public/height:100/width:100/resizing_type:fill/plain/local:///${projectRoot}/data/sadcat.jpg`,
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
    )
  })

  it('will render a public image in all supported formats', async () => {
    const formats = ['origin', 'webp', 'avif']
    const { client: testClient, get: getSpy } = createMockRendererClient()
    vi.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testClient)

    for (let format of formats) {
      const response = await appInstance.inject({
        method: 'GET',
        url: `/render/image/public/public-bucket-2/favicon.ico?format=${format}&width=100&height=100`,
      })

      expect(response.statusCode).toBe(200)
      expect(S3Backend.prototype.privateAssetUrl).toHaveBeenCalledTimes(1)
      const expectFormat = format === 'origin' ? '' : `/format:${format}`
      expect(getSpy).toHaveBeenCalledWith(
        `/public/height:100/width:100/resizing_type:fill${expectFormat}/plain/local:///${projectRoot}/data/sadcat.jpg`,
        expect.objectContaining({
          signal: expect.any(AbortSignal),
        })
      )
      vi.clearAllMocks()
    }
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

    const { client: testClient, get: getSpy } = createMockRendererClient()
    vi.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testClient)

    const response = await appInstance.inject({
      method: 'GET',
      url: signedURLBody.signedURL,
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toHaveBeenCalledTimes(1)
    expect(getSpy).toHaveBeenCalledWith(
      `/public/height:100/width:100/resizing_type:fit/plain/local:///${projectRoot}/data/sadcat.jpg`,
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
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

    const { client: testClient, get: getSpy } = createMockRendererClient()
    vi.spyOn(ImageRenderer.prototype, 'getClient').mockReturnValue(testClient)

    const response = await appInstance.inject({
      method: 'GET',
      url: signedURLBody.signedURL,
    })

    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.privateAssetUrl).toHaveBeenCalledTimes(1)
    expect(getSpy).toHaveBeenCalledWith(
      `/public/height:100/width:100/resizing_type:fit/plain/local:///${projectRoot}/data/sadcat.jpg`,
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
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

  describe('transformation parameter validation', () => {
    it('rejects format parameter with newline character in info route', async () => {
      const response = await appInstance.inject({
        method: 'GET',
        url: '/object/info/public/public-bucket-2/favicon.ico?format=avif%0Amalicious',
      })

      expect(response.statusCode).toBe(400)
      const body = response.json<{ error: string; message: string }>()
      expect(body.message).toContain('format')
      expect(body.message).toContain('must be equal to one of the allowed values')
    })

    it('rejects resize parameter with newline character in HEAD route', async () => {
      const response = await appInstance.inject({
        method: 'HEAD',
        url: '/object/public/public-bucket-2/favicon.ico?resize=cover%0Amalicious',
      })

      expect(response.statusCode).toBe(400)
    })

    it('accepts valid transformation parameters in info route', async () => {
      const response = await appInstance.inject({
        method: 'GET',
        url: '/object/info/public/public-bucket-2/favicon.ico?width=100&height=200',
      })

      expect(response.statusCode).toBe(200)
    })
  })
})
