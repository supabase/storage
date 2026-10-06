import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FileBackend } from '@storage/backend/file'
import fastify, { FastifyInstance, FastifyReply } from 'fastify'
import { getConfig } from '../../config'
import { setErrorHandler } from '../../http/error-handler'
import getPublicObject from '../../http/routes/object/getPublicObject'
import { errorSchema } from '../../http/schemas/error'
import { AssetRenderer } from './asset'
import { AssetResponse, Renderer } from './renderer'

vi.mock('fs-xattr', () => ({ getAttributeSync: () => undefined }))

class TestRenderer extends Renderer {
  async getAsset(): Promise<AssetResponse> {
    return { metadata: {} as AssetResponse['metadata'] }
  }

  contentDisposition(download?: string) {
    const headers: Record<string, string> = {}
    const response = {
      header(name: string, value: string) {
        headers[name.toLowerCase()] = value
        return this
      },
    } as unknown as FastifyReply

    this.handleDownload(response, download)
    return headers['content-disposition']
  }
}

// RFC 8187 attr-char: the only characters allowed unencoded in an ext-value.
const RFC8187_EXT_VALUE = /^UTF-8''(?:[A-Za-z0-9!#$&+\-.^_`|~]|%[0-9A-F]{2})*$/

// RFC 6266 `filename` is a token or a quoted-string (RFC 9110 tchar / qdtext).
const CONTENT_DISPOSITION =
  /^attachment; filename=(?:([!#$%&'*+\-.^_`|~0-9A-Za-z]+)|"((?:[^"\\]|\\.)*)"); filename\*=(\S+)$/

function parseContentDisposition(header: string) {
  const match = CONTENT_DISPOSITION.exec(header)
  const hasControlCharacter = [...header].some((c) => c < ' ' || c === '\x7f')
  if (!match || hasControlCharacter) {
    throw new Error(`malformed Content-Disposition: ${header}`)
  }

  const [, token, quoted, extValue] = match
  return {
    filename: token ?? quoted.replace(/\\(.)/g, '$1'),
    extValue,
    decodedFilename: decodeURIComponent(extValue.slice("UTF-8''".length)),
  }
}

describe('Renderer download Content-Disposition', () => {
  const renderer = new TestRenderer()

  it('does not set Content-Disposition when download is not requested', () => {
    expect(renderer.contentDisposition(undefined)).toBeUndefined()
  })

  it('keeps the existing header for a name that is already a valid token', () => {
    expect(renderer.contentDisposition('report.pdf')).toBe(
      "attachment; filename=report.pdf; filename*=UTF-8''report.pdf"
    )
  })

  it('keeps a bare attachment disposition for an empty download name', () => {
    expect(renderer.contentDisposition('')).toBe('attachment;')
  })

  it.each([
    ['report.pdf', 'report.pdf'],
    ['my file.pdf', 'my file.pdf'],
    ["John's Resume.pdf", "John's Resume.pdf"],
    ['report(1).pdf', 'report(1).pdf'],
    ['a*b.txt', 'a*b.txt'],
    ['a"b\\c.txt', 'a_b_c.txt'],
    ['a\x7fb.txt', 'a_b.txt'],
    ['😀.png', '_.png'],
  ])('encodes the name %j so both parameters decode correctly', (download, fallback) => {
    const header = renderer.contentDisposition(download)
    const parsed = parseContentDisposition(header)

    expect(parsed.extValue).toMatch(RFC8187_EXT_VALUE)
    expect(parsed.decodedFilename).toBe(download)
    expect(parsed.filename).toBe(fallback)
  })

  it('percent-encodes non-ASCII names in filename* and uses an ASCII fallback in filename', () => {
    const header = renderer.contentDisposition('naïve café.txt')
    const parsed = parseContentDisposition(header)

    expect(parsed.extValue).toMatch(RFC8187_EXT_VALUE)
    expect(parsed.decodedFilename).toBe('naïve café.txt')
    expect(parsed.filename).toBe('na_ve caf_.txt')
  })

  it('keeps control characters out of the header value', () => {
    const header = renderer.contentDisposition('evil\r\nSet-Cookie: a=b.txt')

    expect(header).not.toMatch(/[\r\n]/)
    const parsed = parseContentDisposition(header)
    expect(parsed.extValue).toMatch(RFC8187_EXT_VALUE)
    expect(parsed.decodedFilename).toBe('evil\r\nSet-Cookie: a=b.txt')
  })
})

describe('AssetRenderer public download preconditions', () => {
  let app: FastifyInstance
  let directory: string
  let backend: FileBackend

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'storage-download-preconditions-'))
    backend = new FileBackend()
    backend.filePath = directory
    backend.etagAlgorithm = 'md5'
    const key = 'tenant/bucket/object.txt'
    const { storageS3Bucket } = getConfig()
    await mkdir(join(directory, storageS3Bucket, 'tenant/bucket'), { recursive: true })
    await writeFile(join(directory, storageS3Bucket, key), 'current object bytes')

    const renderer = new AssetRenderer(backend)
    const storage = {
      asSuperUser: () => storage,
      findBucket: async () => ({ id: 'bucket', public: true }),
      from: () => ({ findObject: async () => ({ id: 'object', version: undefined }) }),
      location: { getKeyLocation: () => key },
      renderer: () => renderer,
    }
    app = fastify()
    app.addSchema(errorSchema)
    setErrorHandler(app)
    app.decorateRequest('storage')
    app.decorateRequest('tenantId', 'tenant')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.storage = storage as never
      request.signals = { disconnect: new AbortController() } as never
    })
    await app.register(getPublicObject, { prefix: '/object' })
  })

  afterEach(async () => {
    await app?.close()
    if (directory) await rm(directory, { recursive: true, force: true })
  })

  it.each([
    ['If-Match', 'if-match', '"stale-etag"'],
    ['If-Unmodified-Since', 'if-unmodified-since', 'Sat, 01 Jan 2000 00:00:00 GMT'],
  ])('checks %s before range and cache', async (_name, header, value) => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: {
        range: 'bytes=0-6',
        'if-none-match': original.headers.etag as string,
        [header]: value,
      },
    })

    expect(response.statusCode).toBe(412)
    expect(response.json()).toMatchObject({ code: 'PreconditionFailed' })
  })

  it.each([
    [412, 'PreconditionFailed', { 'if-match': '"stale-etag"' }],
    [416, 'InvalidRange', { range: 'bytes=999-1000' }],
  ])('maps an S3 backend %i to %s', async (status, code, headers) => {
    vi.spyOn(backend, 'getObject').mockRejectedValue({
      name: code,
      $metadata: { httpStatusCode: status },
    })

    const response = await app.inject({ url: '/object/public/bucket/object.txt', headers })

    expect(response.statusCode).toBe(status)
    expect(response.json()).toMatchObject({ statusCode: `${status}`, code })
  })

  it('serves the requested range when If-Match matches, ignoring an older date', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    expect(original.statusCode).toBe(200)
    expect(original.body).toBe('current object bytes')

    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: {
        range: 'bytes=0-6',
        'if-match': original.headers.etag as string,
        'if-unmodified-since': 'Sat, 01 Jan 2000 00:00:00 GMT',
      },
    })

    expect(response.statusCode).toBe(206)
    expect(response.body).toBe('current')
  })

  it('preserves If-None-Match revalidation', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: { 'if-none-match': original.headers.etag as string },
    })

    expect(response.statusCode).toBe(304)
    expect(response.body).toBe('')
  })
})
