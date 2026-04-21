import * as fsp from 'node:fs/promises'
import { removePath } from '@internal/fs'
import * as xattr from 'fs-xattr'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import { type Mock, type MockInstance, vi } from 'vitest'
import { getConfig } from '../../config'
import { withOptionalVersion } from './adapter'
import { FileBackend } from './file'

vi.mock('fs-xattr', () => ({
  set: vi.fn(() => Promise.resolve()),
  get: vi.fn(() => Promise.resolve(undefined)),
}))

describe('FileBackend xattr metadata', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('uses a distinct linux xattr key for etag', async () => {
    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    const originalPlatformDescriptor = Object.getOwnPropertyDescriptor(process, 'platform')
    const originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    const originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH

    try {
      Object.defineProperty(process, 'platform', {
        value: 'linux',
        configurable: true,
      })
      process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
      process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
      getConfig({ reload: true })

      const backend = new FileBackend()
      const uploadId = await backend.createMultiPartUpload(
        'bucket',
        'key',
        'v1',
        'text/plain',
        'no-cache'
      )

      await backend.uploadPart('bucket', 'key', 'v1', uploadId as string, 1, Readable.from('hello'))

      expect(xattr.set).toHaveBeenCalledWith(
        expect.any(String),
        'user.supabase.etag',
        expect.any(String)
      )
    } finally {
      if (originalPlatformDescriptor) {
        Object.defineProperty(process, 'platform', originalPlatformDescriptor)
      }
      if (originalStoragePath === undefined) {
        delete process.env.STORAGE_FILE_BACKEND_PATH
      } else {
        process.env.STORAGE_FILE_BACKEND_PATH = originalStoragePath
      }
      if (originalFilePath === undefined) {
        delete process.env.FILE_STORAGE_BACKEND_PATH
      } else {
        process.env.FILE_STORAGE_BACKEND_PATH = originalFilePath
      }
      await removePath(tmpDir)
    }
  })

  it('reads linux etag xattr during multipart completion', async () => {
    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    const originalPlatformDescriptor = Object.getOwnPropertyDescriptor(process, 'platform')
    const originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    const originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    let uploadSpy: MockInstance | undefined

    try {
      Object.defineProperty(process, 'platform', {
        value: 'linux',
        configurable: true,
      })
      process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
      process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
      getConfig({ reload: true })

      const backend = new FileBackend()
      const uploadId = await backend.createMultiPartUpload(
        'bucket',
        'key',
        'v1',
        'text/plain',
        'no-cache'
      )

      const partDir = path.join(
        tmpDir,
        'multiparts',
        uploadId as string,
        'bucket',
        withOptionalVersion('key', 'v1')
      )
      const partPath = path.join(partDir, 'part-1')
      await fsp.mkdir(partDir, { recursive: true })
      await fsp.writeFile(partPath, 'hello')

      const xattrGet = xattr.get as unknown as Mock
      xattrGet.mockImplementation((_file: string, attribute: string) => {
        if (attribute === 'user.supabase.etag') {
          return Promise.resolve(Buffer.from('part-etag'))
        }
        return Promise.resolve(undefined)
      })

      uploadSpy = vi
        .spyOn(backend, 'uploadObject')
        .mockImplementation(async (_bucket, _key, _version, body) => {
          await new Promise<void>((resolve, reject) => {
            body.on('error', reject)
            body.on('end', resolve)
            body.resume()
          })
          return {
            httpStatusCode: 200,
            size: 5,
            cacheControl: 'no-cache',
            mimetype: 'text/plain',
            eTag: '"final"',
            lastModified: new Date(),
            contentLength: 5,
          }
        })

      await expect(
        backend.completeMultipartUpload('bucket', 'key', uploadId as string, 'v1', [
          { PartNumber: 1, ETag: 'part-etag' },
        ])
      ).resolves.toMatchObject({
        ETag: '"final"',
      })

      expect(xattr.get).toHaveBeenCalledWith(expect.any(String), 'user.supabase.etag')
    } finally {
      uploadSpy?.mockRestore()
      if (originalPlatformDescriptor) {
        Object.defineProperty(process, 'platform', originalPlatformDescriptor)
      }
      if (originalStoragePath === undefined) {
        delete process.env.STORAGE_FILE_BACKEND_PATH
      } else {
        process.env.STORAGE_FILE_BACKEND_PATH = originalStoragePath
      }
      if (originalFilePath === undefined) {
        delete process.env.FILE_STORAGE_BACKEND_PATH
      } else {
        process.env.FILE_STORAGE_BACKEND_PATH = originalFilePath
      }
      await removePath(tmpDir)
    }
  })
})

describe('FileBackend traversal protection', () => {
  let tmpDir: string
  let backend: FileBackend
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  let escapePrefix: string

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()
    escapePrefix = `storage-traversal-${Date.now()}-${Math.random().toString(36).slice(2)}`
  })

  afterEach(async () => {
    if (originalStoragePath === undefined) {
      delete process.env.STORAGE_FILE_BACKEND_PATH
    } else {
      process.env.STORAGE_FILE_BACKEND_PATH = originalStoragePath
    }
    if (originalFilePath === undefined) {
      delete process.env.FILE_STORAGE_BACKEND_PATH
    } else {
      process.env.FILE_STORAGE_BACKEND_PATH = originalFilePath
    }

    await removePath(tmpDir)
    await removePath(path.join('/tmp', escapePrefix))
  })

  it('rejects traversal key in multipart create with InvalidKey', async () => {
    const traversalKey = `${'../'.repeat(20)}tmp/${escapePrefix}/multipart-escape.txt`
    await expect(
      backend.createMultiPartUpload('bucket', traversalKey, 'v1', 'text/plain', 'no-cache')
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })
  })

  it('rejects traversal key in multipart upload-part with InvalidKey', async () => {
    const traversalKey = `${'../'.repeat(20)}tmp/${escapePrefix}/multipart-escape.txt`
    await expect(
      backend.uploadPart('bucket', traversalKey, 'v1', 'upload-id', 1, Readable.from('escape-part'))
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })
  })

  it('rejects traversal key in object operations with InvalidKey', async () => {
    const traversalKey = `${'../'.repeat(20)}tmp/${escapePrefix}/object-escape.txt`

    await expect(
      backend.uploadObject(
        'bucket',
        traversalKey,
        'v1',
        Readable.from('escape'),
        'text/plain',
        'no-cache'
      )
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(backend.headObject('bucket', traversalKey, 'v1')).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(backend.getObject('bucket', traversalKey, 'v1')).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(backend.deleteObject('bucket', traversalKey, 'v1')).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(backend.privateAssetUrl('bucket', traversalKey, 'v1')).rejects.toMatchObject({
      code: 'InvalidKey',
    })
  })

  it('rejects traversal key in copy/delete list operations with InvalidKey', async () => {
    const traversalKey = `${'../'.repeat(20)}tmp/${escapePrefix}/copy-escape.txt`

    await backend.uploadObject(
      'bucket',
      'safe-source.txt',
      'v1',
      Readable.from('safe-source'),
      'text/plain',
      'no-cache'
    )

    await expect(
      backend.copyObject('bucket', 'safe-source.txt', 'v1', traversalKey, 'v2', {})
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(backend.deleteObjects('bucket', [traversalKey])).rejects.toMatchObject({
      code: 'InvalidKey',
    })
  })

  it('rejects traversal key in multipart auxiliary operations with InvalidKey', async () => {
    const traversalDestKey = `${'../'.repeat(20)}tmp/${escapePrefix}/multipart-dest-escape.txt`
    const traversalSourceKey = `${'../'.repeat(20)}tmp/${escapePrefix}/multipart-source-escape.txt`

    await expect(
      backend.abortMultipartUpload('bucket', 'key', traversalDestKey)
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(
      backend.uploadPartCopy(
        'bucket',
        traversalDestKey,
        'v1',
        'upload-id',
        1,
        'safe-source.txt',
        'v1'
      )
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })

    await expect(
      backend.uploadPartCopy(
        'bucket',
        'safe-dest.txt',
        'v1',
        'upload-id',
        1,
        traversalSourceKey,
        'v1'
      )
    ).rejects.toMatchObject({
      code: 'InvalidKey',
    })
  })
})

describe('FileBackend lastModified', () => {
  let tmpDir: string
  let backend: FileBackend
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()
  })

  afterEach(async () => {
    if (originalStoragePath === undefined) {
      delete process.env.STORAGE_FILE_BACKEND_PATH
    } else {
      process.env.STORAGE_FILE_BACKEND_PATH = originalStoragePath
    }
    if (originalFilePath === undefined) {
      delete process.env.FILE_STORAGE_BACKEND_PATH
    } else {
      process.env.FILE_STORAGE_BACKEND_PATH = originalFilePath
    }
    await removePath(tmpDir)
  })

  it('headObject/getObject should return mtime as lastModified', async () => {
    const bucket = 'test-bucket'
    const key = 'test-file.txt'
    const version = 'v1'

    await backend.uploadObject(
      bucket,
      key,
      version,
      Readable.from('initial content'),
      'text/plain',
      'no-cache'
    )

    const filePath = path.join(tmpDir, withOptionalVersion(`${bucket}/${key}`, version))
    const stat = await fsp.stat(filePath)
    const knownMtime = new Date(stat.birthtimeMs + 60_000) // mtime must be in the future
    await fsp.utimes(filePath, knownMtime, knownMtime)

    const headResult = await backend.headObject(bucket, key, version)
    expect(headResult.lastModified).toEqual(knownMtime)

    const getResult = await backend.getObject(bucket, key, version)
    expect(getResult.metadata.lastModified).toEqual(knownMtime)
  })
})
