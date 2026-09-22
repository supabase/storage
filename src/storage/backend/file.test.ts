import fs from 'node:fs'
import * as fsp from 'node:fs/promises'
import { ErrorCode } from '@internal/errors/codes'
import { removePath } from '@internal/fs'
import * as xattr from 'fs-xattr'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import { text } from 'stream/consumers'
import { type Mock, type MockInstance, vi } from 'vitest'
import { getConfig } from '../../config'
import { withOptionalVersion } from './adapter'
import { FileBackend } from './file'

vi.mock('fs-xattr', () => ({
  setAttributeSync: vi.fn(() => undefined),
  getAttributeSync: vi.fn(() => undefined),
  removeAttributeSync: vi.fn(() => undefined),
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

      expect(xattr.setAttributeSync).toHaveBeenCalledWith(
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

      const xattrGet = xattr.getAttributeSync as unknown as Mock
      xattrGet.mockImplementation((_file: string, attribute: string) => {
        if (attribute === 'user.supabase.etag') {
          return Buffer.from('part-etag')
        }
        return undefined
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

      expect(xattr.getAttributeSync).toHaveBeenCalledWith(expect.any(String), 'user.supabase.etag')
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

describe('FileBackend bulk deletion outcomes', () => {
  let tmpDir: string
  let backend: FileBackend

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-delete-'))
    vi.stubEnv('STORAGE_FILE_BACKEND_PATH', tmpDir)
    vi.stubEnv('FILE_STORAGE_BACKEND_PATH', tmpDir)
    getConfig({ reload: true })
    backend = new FileBackend()
    await fsp.mkdir(path.join(tmpDir, 'bucket', 'folder'), { recursive: true })
    await fsp.writeFile(path.join(tmpDir, 'bucket', 'folder', 'object'), 'data')
  })

  afterEach(async () => {
    vi.unstubAllEnvs()
    getConfig({ reload: true })
    await removePath(tmpDir)
  })

  it('confirms deleted and absent keys while cleaning empty parents', async () => {
    await expect(
      backend.deleteObjectsDetailed('bucket', ['folder/missing', 'folder/object'])
    ).resolves.toEqual([
      { key: 'folder/missing', outcome: 'DELETED' },
      { key: 'folder/object', outcome: 'DELETED' },
    ])
    await expect(fsp.access(path.join(tmpDir, 'bucket'))).rejects.toMatchObject({ code: 'ENOENT' })
    await expect(fsp.access(tmpDir)).resolves.toBeUndefined()
    await expect(backend.deleteObjects('bucket', ['folder/object'])).resolves.toBeUndefined()
  })

  it('reports filesystem failures without hiding successful deletions', async () => {
    await fsp.writeFile(path.join(tmpDir, 'bucket', 'blocked'), 'not a directory')

    const results = await backend.deleteObjectsDetailed('bucket', [
      'blocked/child',
      'folder/object',
    ])

    expect(results).toEqual([
      {
        key: 'blocked/child',
        outcome: 'UNKNOWN',
        error: { code: 'ENOTDIR', message: expect.any(String) },
      },
      { key: 'folder/object', outcome: 'DELETED' },
    ])
    await expect(fsp.access(path.join(tmpDir, 'bucket', 'folder'))).rejects.toMatchObject({
      code: 'ENOENT',
    })
    await expect(fsp.readFile(path.join(tmpDir, 'bucket', 'blocked'), 'utf8')).resolves.toBe(
      'not a directory'
    )
  })

  it('rethrows the original filesystem error from the existing wrapper', async () => {
    await fsp.writeFile(path.join(tmpDir, 'bucket', 'blocked'), 'not a directory')
    const filesystem = await import('@internal/fs')
    const remove = filesystem.removePath
    let originalError: unknown
    const removePathSpy = vi.spyOn(filesystem, 'removePath').mockImplementation(async (...args) => {
      try {
        await remove(...args)
      } catch (error) {
        originalError = error
        throw error
      }
    })

    try {
      const error = await backend.deleteObjects('bucket', ['folder/object', 'blocked/child']).then(
        () => undefined,
        (error: unknown) => error
      )
      expect(error).toBe(originalError)
      expect(error).toMatchObject({
        code: 'ENOTDIR',
        errno: expect.any(Number),
        syscall: expect.any(String),
        path: path.join(tmpDir, 'bucket', 'blocked', 'child'),
      })
    } finally {
      removePathSpy.mockRestore()
    }
    await expect(fsp.access(path.join(tmpDir, 'bucket', 'folder'))).rejects.toMatchObject({
      code: 'ENOENT',
    })
  })

  it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
    'keeps a recursive permission failure unknown after partially deleting a directory',
    async () => {
      const target = path.join(tmpDir, 'bucket', 'partial')
      const locked = path.join(target, 'z-locked')
      await fsp.mkdir(locked, { recursive: true })
      await fsp.writeFile(path.join(target, 'a-removable'), 'deleted first')
      await fsp.writeFile(path.join(locked, 'kept'), 'protected')
      await fsp.chmod(locked, 0)
      try {
        await expect(backend.deleteObjectsDetailed('bucket', ['partial'])).resolves.toEqual([
          {
            key: 'partial',
            outcome: 'UNKNOWN',
            error: { code: 'EACCES', message: expect.any(String) },
          },
        ])
        await vi.waitFor(async () => {
          await expect(fsp.access(path.join(target, 'a-removable'))).rejects.toMatchObject({
            code: 'ENOENT',
          })
        })
      } finally {
        await fsp.chmod(locked, 0o700)
      }
      expect(await fsp.readFile(path.join(locked, 'kept'), 'utf8')).toBe('protected')
    }
  )

  it('validates all paths before deleting a mixed valid and invalid batch', async () => {
    await expect(
      backend.deleteObjectsDetailed('bucket', ['folder/object', '../../outside'])
    ).rejects.toMatchObject({ code: 'InvalidKey' })
    await expect(
      fsp.readFile(path.join(tmpDir, 'bucket', 'folder', 'object'), 'utf8')
    ).resolves.toBe('data')
  })

  it('returns an empty result without removing directories', async () => {
    await expect(backend.deleteObjectsDetailed('bucket', [])).resolves.toEqual([])
    await expect(backend.deleteObjects('bucket', [])).resolves.toBeUndefined()
    await expect(fsp.access(path.join(tmpDir, 'bucket', 'folder'))).resolves.toBeUndefined()
  })
})

describe('FileBackend empty directory cleanup', () => {
  let tmpDir: string
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  let siblingDirectory: string | undefined

  class TestFileBackend extends FileBackend {
    async cleanup(dirPath: string) {
      await this.cleanupEmptyDirectories(dirPath)
    }
  }

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
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
    if (siblingDirectory) {
      await removePath(siblingDirectory)
    }
  })

  it('preserves a directory repopulated by an upload', async () => {
    const backend = new TestFileBackend()
    const objectDirectory = path.join(tmpDir, 'bucket', 'object.jpg')
    const version = 'new-version'
    await fsp.mkdir(objectDirectory, { recursive: true })
    await fsp.writeFile(path.join(objectDirectory, version), 'new upload')

    await backend.cleanup(objectDirectory)

    await expect(fsp.readFile(path.join(objectDirectory, version), 'utf8')).resolves.toBe(
      'new upload'
    )
  })

  it('removes empty directories recursively up to the storage root', async () => {
    const backend = new TestFileBackend()
    const bucketDirectory = path.join(tmpDir, 'bucket')
    const objectDirectory = path.join(bucketDirectory, 'nested', 'object.jpg')
    await fsp.mkdir(objectDirectory, { recursive: true })

    await backend.cleanup(objectDirectory)

    await expect(fsp.access(bucketDirectory)).rejects.toMatchObject({ code: 'ENOENT' })
    await expect(fsp.access(tmpDir)).resolves.toBeUndefined()
  })

  it('continues cleaning parents when the target directory is already absent', async () => {
    const backend = new TestFileBackend()
    const bucketDirectory = path.join(tmpDir, 'bucket')
    const objectDirectory = path.join(bucketDirectory, 'nested', 'object.jpg')
    await fsp.mkdir(path.dirname(objectDirectory), { recursive: true })

    await backend.cleanup(objectDirectory)

    await expect(fsp.access(bucketDirectory)).rejects.toMatchObject({ code: 'ENOENT' })
    await expect(fsp.access(tmpDir)).resolves.toBeUndefined()
  })

  it('does not clean a sibling directory that shares the storage-root prefix', async () => {
    const backend = new TestFileBackend()
    siblingDirectory = `${tmpDir}-sibling`
    await fsp.mkdir(siblingDirectory)

    await backend.cleanup(siblingDirectory)

    await expect(fsp.access(siblingDirectory)).resolves.toBeUndefined()
  })
})

describe('FileBackend copy metadata options', () => {
  let tmpDir: string
  let backend: FileBackend
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  let originalPlatformDescriptor: PropertyDescriptor | undefined

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    originalPlatformDescriptor = Object.getOwnPropertyDescriptor(process, 'platform')
    Object.defineProperty(process, 'platform', {
      value: 'linux',
      configurable: true,
    })
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()

    const xattrGet = xattr.getAttributeSync as unknown as Mock
    xattrGet.mockReset()
    xattrGet.mockImplementation((_file: string, attribute: string) => {
      if (attribute === 'user.supabase.cache-control') {
        return Buffer.from('max-age=60')
      }
      if (attribute === 'user.supabase.content-type') {
        return Buffer.from('text/plain')
      }
      return undefined
    })
    ;(xattr.setAttributeSync as unknown as Mock).mockReset()
    ;(xattr.removeAttributeSync as unknown as Mock).mockReset()

    await backend.uploadObject(
      'bucket',
      'source.txt',
      'v1',
      Readable.from('source-body'),
      'text/plain',
      'max-age=60'
    )
    ;(xattr.setAttributeSync as unknown as Mock).mockClear()
    ;(xattr.removeAttributeSync as unknown as Mock).mockClear()
  })

  afterEach(async () => {
    ;(xattr.getAttributeSync as unknown as Mock).mockReset()
    ;(xattr.setAttributeSync as unknown as Mock).mockReset()
    ;(xattr.removeAttributeSync as unknown as Mock).mockReset()
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
  })

  it('preserves source metadata when copyMetadata is true', async () => {
    const setMetadataSpy = vi.spyOn(backend, 'setFileMetadata')

    await backend.copyObject(
      'bucket',
      'source.txt',
      'v1',
      'copy-preserve.txt',
      undefined,
      {
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      },
      undefined,
      { copyMetadata: true }
    )

    expect(setMetadataSpy).toHaveBeenCalledWith(expect.any(String), {
      cacheControl: 'max-age=60',
      contentType: 'text/plain',
    })
  })

  it('overwrites file metadata when copyMetadata is false', async () => {
    const setMetadataSpy = vi.spyOn(backend, 'setFileMetadata')

    await backend.copyObject(
      'bucket',
      'source.txt',
      'v1',
      'copy-replace.txt',
      undefined,
      {
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      },
      undefined,
      { copyMetadata: false }
    )

    expect(setMetadataSpy).toHaveBeenCalledWith(expect.any(String), {
      cacheControl: 'max-age=999',
      contentType: 'image/gif',
    })
  })

  it('removes omitted metadata when copyMetadata is false', async () => {
    const setMetadataSpy = vi.spyOn(backend, 'setFileMetadata')

    await backend.copyObject(
      'bucket',
      'source.txt',
      'v1',
      'copy-partial-replace.txt',
      undefined,
      {
        cacheControl: 'max-age=999',
      },
      undefined,
      { copyMetadata: false }
    )

    expect(setMetadataSpy).toHaveBeenCalledWith(expect.any(String), {
      cacheControl: 'max-age=999',
      contentType: undefined,
    })
    expect(xattr.setAttributeSync).toHaveBeenCalledWith(
      expect.any(String),
      'user.supabase.cache-control',
      'max-age=999'
    )
    expect(xattr.removeAttributeSync).toHaveBeenCalledWith(
      expect.any(String),
      'user.supabase.content-type'
    )
  })

  it('removes all metadata when replacement metadata is empty', async () => {
    await backend.copyObject(
      'bucket',
      'source.txt',
      'v1',
      'copy-empty-replace.txt',
      undefined,
      {},
      undefined,
      { copyMetadata: false }
    )

    expect(xattr.setAttributeSync).not.toHaveBeenCalled()
    expect(xattr.removeAttributeSync).toHaveBeenCalledTimes(2)
    expect(xattr.removeAttributeSync).toHaveBeenCalledWith(
      expect.any(String),
      'user.supabase.cache-control'
    )
    expect(xattr.removeAttributeSync).toHaveBeenCalledWith(
      expect.any(String),
      'user.supabase.content-type'
    )
  })

  it('preserves absent source metadata when copyMetadata is true', async () => {
    const missingXattr = Object.assign(new Error('missing xattr'), { code: 'ENODATA' })
    ;(xattr.getAttributeSync as unknown as Mock).mockImplementation(() => {
      throw missingXattr
    })

    await expect(
      backend.copyObject(
        'bucket',
        'source.txt',
        'v1',
        'copy-without-metadata.txt',
        undefined,
        undefined,
        undefined,
        { copyMetadata: true }
      )
    ).resolves.toMatchObject({ httpStatusCode: 200 })

    expect(xattr.removeAttributeSync).toHaveBeenCalledTimes(2)
  })

  it('ignores already absent destination metadata', async () => {
    const missingXattr = Object.assign(new Error('missing xattr'), { code: 'ENOATTR' })
    ;(xattr.removeAttributeSync as unknown as Mock).mockImplementation(() => {
      throw missingXattr
    })

    await expect(
      backend.copyObject(
        'bucket',
        'source.txt',
        'v1',
        'copy-empty-replace.txt',
        undefined,
        {},
        undefined,
        { copyMetadata: false }
      )
    ).resolves.toMatchObject({ httpStatusCode: 200 })
  })

  it('propagates genuine source metadata read errors', async () => {
    const readError = Object.assign(new Error('xattr read failed'), { code: 'EIO' })
    ;(xattr.getAttributeSync as unknown as Mock).mockImplementation(() => {
      throw readError
    })

    await expect(
      backend.copyObject(
        'bucket',
        'source.txt',
        'v1',
        'copy-read-failure.txt',
        undefined,
        undefined,
        undefined,
        { copyMetadata: true }
      )
    ).rejects.toBe(readError)
  })

  it('propagates genuine destination metadata removal errors', async () => {
    const removeError = Object.assign(new Error('xattr removal failed'), { code: 'EIO' })
    ;(xattr.removeAttributeSync as unknown as Mock).mockImplementation(() => {
      throw removeError
    })

    await expect(
      backend.copyObject(
        'bucket',
        'source.txt',
        'v1',
        'copy-remove-failure.txt',
        undefined,
        {},
        undefined,
        { copyMetadata: false }
      )
    ).rejects.toBe(removeError)
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

describe('FileBackend conditional reads', () => {
  let tmpDir: string
  let backend: FileBackend
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  const bucket = 'conditional-bucket'
  const key = 'conditional.txt'
  const version = 'v1'
  // A realistic mtime with a sub-second component
  const mtime = new Date('2026-01-01T00:00:00.700Z')
  const lastModifiedHeader = mtime.toUTCString()

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()

    await backend.uploadObject(
      bucket,
      key,
      version,
      Readable.from('body'),
      'text/plain',
      'no-cache'
    )
    const filePath = path.join(tmpDir, withOptionalVersion(`${bucket}/${key}`, version))
    await fsp.utimes(filePath, mtime, mtime)
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

  async function statusFor(headers: { ifNoneMatch?: string; ifModifiedSince?: string }) {
    const response = await backend.getObject(bucket, key, version, headers)
    if (response.body instanceof Readable) {
      response.body.destroy()
    }
    return response.httpStatusCode
  }

  it('returns 304 when if-modified-since echoes the Last-Modified header', async () => {
    const head = await backend.headObject(bucket, key, version)
    expect(head.lastModified?.toUTCString()).toBe(lastModifiedHeader)

    await expect(statusFor({ ifModifiedSince: lastModifiedHeader })).resolves.toBe(304)
  })

  it('returns 304 when if-modified-since is later than the last modification', async () => {
    await expect(
      statusFor({ ifModifiedSince: new Date(mtime.getTime() + 60_000).toUTCString() })
    ).resolves.toBe(304)
  })

  it('returns 200 when the object changed after if-modified-since', async () => {
    await expect(
      statusFor({ ifModifiedSince: new Date(mtime.getTime() - 1_000).toUTCString() })
    ).resolves.toBe(200)
  })

  it('returns 304 when if-none-match matches the etag', async () => {
    const head = await backend.headObject(bucket, key, version)
    await expect(statusFor({ ifNoneMatch: head.eTag })).resolves.toBe(304)
  })

  it.each([
    ['a weak tag', (eTag: string) => `W/${eTag}`],
    ['a tag list', (eTag: string) => `"stale-etag", ${eTag}`],
    ['a wildcard', () => '*'],
    ['an unquoted tag', (eTag: string) => eTag.replace(/"/g, '')],
  ])('returns 304 when if-none-match is %s matching the etag', async (_name, toHeader) => {
    const head = await backend.headObject(bucket, key, version)
    await expect(statusFor({ ifNoneMatch: toHeader(head.eTag) })).resolves.toBe(304)
  })

  it('returns 200 when no tag in an if-none-match list matches', async () => {
    await expect(statusFor({ ifNoneMatch: '"stale-etag", W/"other-etag"' })).resolves.toBe(200)
  })

  it('returns 200 when a quoted if-none-match tag contains commas and a wildcard', async () => {
    await expect(statusFor({ ifNoneMatch: '"stale,*,etag"' })).resolves.toBe(200)
  })

  it('ignores if-modified-since when if-none-match is present and does not match', async () => {
    await expect(
      statusFor({ ifNoneMatch: '"stale-etag"', ifModifiedSince: lastModifiedHeader })
    ).resolves.toBe(200)
    await expect(
      statusFor({
        ifNoneMatch: '"stale-etag"',
        ifModifiedSince: new Date(mtime.getTime() + 60_000).toUTCString(),
      })
    ).resolves.toBe(200)
  })

  it('ignores an invalid if-modified-since date', async () => {
    await expect(statusFor({ ifModifiedSince: 'not a date' })).resolves.toBe(200)
  })

  it.each([
    lastModifiedHeader,
    new Date(mtime.getTime() + 60_000).toUTCString(),
  ])('ignores if-modified-since %s when if-none-match is empty', async (ifModifiedSince) => {
    await expect(statusFor({ ifNoneMatch: '', ifModifiedSince })).resolves.toBe(200)
  })
})

describe('FileBackend range reads', () => {
  let tmpDir: string
  let backend: FileBackend
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  const bucket = 'range-bucket'
  const key = 'range.txt'
  const version = 'v1'
  const payload = '0123456789'

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()

    await backend.uploadObject(
      bucket,
      key,
      version,
      Readable.from(payload),
      'text/plain',
      'no-cache'
    )
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

  it('returns inclusive explicit byte ranges', async () => {
    const result = await backend.getObject(bucket, key, version, { range: 'bytes=2-5' })

    await expect(text(result.body as NodeJS.ReadableStream)).resolves.toBe('2345')
    expect(result.httpStatusCode).toBe(206)
    expect(result.metadata.contentRange).toBe('bytes 2-5/10')
    expect(result.metadata.contentLength).toBe(4)
    expect(result.metadata.size).toBe(4)
  })

  it('returns open-ended byte ranges', async () => {
    const result = await backend.getObject(bucket, key, version, { range: 'bytes=7-' })

    await expect(text(result.body as NodeJS.ReadableStream)).resolves.toBe('789')
    expect(result.metadata.contentRange).toBe('bytes 7-9/10')
    expect(result.metadata.contentLength).toBe(3)
    expect(result.metadata.size).toBe(3)
  })

  it('returns suffix byte ranges', async () => {
    const result = await backend.getObject(bucket, key, version, { range: 'bytes=-5' })

    await expect(text(result.body as NodeJS.ReadableStream)).resolves.toBe('56789')
    expect(result.metadata.contentRange).toBe('bytes 5-9/10')
    expect(result.metadata.contentLength).toBe(5)
    expect(result.metadata.size).toBe(5)
  })

  it('caps range ends at the object size', async () => {
    const result = await backend.getObject(bucket, key, version, { range: 'bytes=8-99' })

    await expect(text(result.body as NodeJS.ReadableStream)).resolves.toBe('89')
    expect(result.metadata.contentRange).toBe('bytes 8-9/10')
    expect(result.metadata.contentLength).toBe(2)
    expect(result.metadata.size).toBe(2)
  })

  it.each([
    'bytes=-0',
    'bytes=10-12',
    'bytes=8-4',
    'bytes=-',
    'bytes=a-b',
    'items=0-1',
  ])('rejects invalid byte range %s', async (range) => {
    await expect(backend.getObject(bucket, key, version, { range })).rejects.toMatchObject({
      code: ErrorCode.InvalidRange,
      error: 'invalid_range',
      httpStatusCode: 416,
      userStatusCode: 416,
      message: 'invalid range provided',
    })
  })
})

describe('FileBackend copy source preconditions', () => {
  let tmpDir: string
  let originalStoragePath: string | undefined
  let originalFilePath: string | undefined
  let backend: FileBackend
  let sourceETag: string
  let sourceLastModified: Date
  const sourceMtime = new Date('2026-01-01T00:00:00.700Z')

  const copy = (conditions: Parameters<FileBackend['copyObject']>[6]) =>
    backend.copyObject('bucket', 'source.txt', 'v1', 'destination.txt', 'v1', undefined, conditions)

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    process.env.STORAGE_FILE_BACKEND_PATH = tmpDir
    process.env.FILE_STORAGE_BACKEND_PATH = tmpDir
    getConfig({ reload: true })
    backend = new FileBackend()

    await backend.uploadObject(
      'bucket',
      'source.txt',
      'v1',
      Readable.from('source-body'),
      'text/plain',
      'no-cache'
    )
    await fsp.utimes(
      path.join(tmpDir, withOptionalVersion('bucket/source.txt', 'v1')),
      sourceMtime,
      sourceMtime
    )
    const source = await backend.headObject('bucket', 'source.txt', 'v1')
    sourceETag = source.eTag
    sourceLastModified = source.lastModified as Date
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

  async function expectPreconditionFailed(conditions: Parameters<typeof copy>[0]) {
    await expect(copy(conditions)).rejects.toMatchObject({
      httpStatusCode: 412,
      message: 'PreconditionFailed',
    })
    await expect(
      fsp.stat(path.join(tmpDir, withOptionalVersion('bucket/destination.txt', 'v1')))
    ).rejects.toMatchObject({ code: 'ENOENT' })
  }

  async function expectCopied(conditions: Parameters<typeof copy>[0]) {
    await expect(copy(conditions)).resolves.toMatchObject({ httpStatusCode: 200 })
    expect(
      await fsp.readFile(
        path.join(tmpDir, withOptionalVersion('bucket/destination.txt', 'v1')),
        'utf8'
      )
    ).toBe('source-body')
  }

  it('rejects the copy when if-match does not match the source etag', async () => {
    await expectPreconditionFailed({ ifMatch: '"not-the-source-etag"' })
  })

  it('copies when if-match matches the source etag', async () => {
    await expectCopied({ ifMatch: sourceETag })
  })

  it('rejects the copy when if-none-match matches the source etag', async () => {
    await expectPreconditionFailed({ ifNoneMatch: sourceETag })
  })

  it('rejects the copy when the source was modified after if-unmodified-since', async () => {
    await expectPreconditionFailed({
      ifUnmodifiedSince: new Date(sourceLastModified.getTime() - 60_000),
    })
  })

  it('copies when the source was not modified after if-unmodified-since', async () => {
    await expectCopied({
      ifUnmodifiedSince: new Date(sourceLastModified.getTime() + 60_000),
    })
  })

  it('rejects the copy when the source was not modified after if-modified-since', async () => {
    await expectPreconditionFailed({
      ifModifiedSince: new Date(sourceLastModified.getTime() + 60_000),
    })
  })

  it('copies when the source was modified after if-modified-since', async () => {
    await expectCopied({
      ifModifiedSince: new Date(sourceLastModified.getTime() - 60_000),
    })
  })

  it('copies when if-match is true even if if-unmodified-since is false', async () => {
    await expectCopied({
      ifMatch: sourceETag,
      ifUnmodifiedSince: new Date(sourceLastModified.getTime() - 60_000),
    })
  })

  it('rejects the copy when if-none-match is false even if if-modified-since is true', async () => {
    await expectPreconditionFailed({
      ifNoneMatch: sourceETag,
      ifModifiedSince: new Date(sourceLastModified.getTime() - 60_000),
    })
  })

  it('rejects the copy when both if-match and if-none-match match the source', async () => {
    await expectPreconditionFailed({ ifMatch: sourceETag, ifNoneMatch: sourceETag })
  })

  it.each([
    { name: 'absent', conditions: undefined },
    { name: 'empty', conditions: {} },
    {
      name: 'all undefined',
      conditions: {
        ifMatch: undefined,
        ifNoneMatch: undefined,
        ifModifiedSince: undefined,
        ifUnmodifiedSince: undefined,
      },
    },
    {
      name: 'date-only',
      conditions: {
        ifModifiedSince: new Date('2025-01-01T00:00:00.000Z'),
        ifUnmodifiedSince: new Date('2027-01-01T00:00:00.000Z'),
      },
    },
  ])('does not hash the source when conditions are $name', async ({ conditions }) => {
    backend.etagAlgorithm = 'md5'
    const createReadStream = vi.spyOn(fs, 'createReadStream')

    try {
      await expectCopied(conditions)
      expect(createReadStream).toHaveBeenCalledWith(
        path.join(tmpDir, withOptionalVersion('bucket/destination.txt', 'v1'))
      )
      expect(createReadStream.mock.calls.map(([file]) => file)).not.toContain(
        path.join(tmpDir, withOptionalVersion('bucket/source.txt', 'v1'))
      )
    } finally {
      createReadStream.mockRestore()
    }
  })

  it('ignores invalid precondition dates', async () => {
    await expectCopied({
      ifModifiedSince: new Date('invalid'),
      ifUnmodifiedSince: new Date('invalid'),
    })
  })

  it.each([
    { name: 'if-match', conditions: () => ({ ifMatch: '"not-the-source-etag"' }) },
    { name: 'if-none-match', conditions: () => ({ ifNoneMatch: sourceETag }) },
    {
      name: 'if-modified-since',
      conditions: () => ({ ifModifiedSince: new Date(sourceLastModified.getTime() + 60_000) }),
    },
    {
      name: 'if-unmodified-since',
      conditions: () => ({ ifUnmodifiedSince: new Date(sourceLastModified.getTime() - 60_000) }),
    },
  ])('preserves an existing destination when $name fails', async ({ conditions }) => {
    await backend.uploadObject(
      'bucket',
      'destination.txt',
      'v1',
      Readable.from('original-destination-body'),
      'application/json',
      'max-age=60'
    )
    const originalMetadata = await backend.headObject('bucket', 'destination.txt', 'v1')
    vi.mocked(xattr.setAttributeSync).mockClear()
    vi.mocked(xattr.removeAttributeSync).mockClear()

    await expect(copy(conditions())).rejects.toMatchObject({
      httpStatusCode: 412,
      message: 'PreconditionFailed',
    })

    expect(
      await fsp.readFile(
        path.join(tmpDir, withOptionalVersion('bucket/destination.txt', 'v1')),
        'utf8'
      )
    ).toBe('original-destination-body')
    await expect(backend.headObject('bucket', 'destination.txt', 'v1')).resolves.toEqual(
      originalMetadata
    )
    expect(xattr.setAttributeSync).not.toHaveBeenCalled()
    expect(xattr.removeAttributeSync).not.toHaveBeenCalled()
  })

  it.each([
    ['a wildcard', () => '*'],
    ['a weak etag', () => `W/${sourceETag}`],
    ['an etag list', () => `"not-the-source-etag", ${sourceETag}`],
    ['an unquoted etag', () => sourceETag.replace(/"/g, '')],
  ])('copies when if-match is %s', async (_, ifMatch) => {
    await expectCopied({ ifMatch: ifMatch() })
  })

  it.each([
    'W/"not-the-source-etag"',
    '"not-the-source-etag", "another-etag"',
    'not-the-source-etag',
    '"not-the,*,source-etag"',
  ])('rejects the copy when if-match %s does not match the source', async (ifMatch) => {
    await expectPreconditionFailed({ ifMatch })
  })

  it.each([
    ['a wildcard', () => '*'],
    ['a weak etag', () => `W/${sourceETag}`],
    ['an etag list', () => `"not-the-source-etag", ${sourceETag}`],
    ['an unquoted etag', () => sourceETag.replace(/"/g, '')],
  ])('rejects the copy when if-none-match is %s', async (_, ifNoneMatch) => {
    await expectPreconditionFailed({ ifNoneMatch: ifNoneMatch() })
  })

  it.each([
    'W/"not-the-source-etag"',
    '"not-the-source-etag", "another-etag"',
    'not-the-source-etag',
    '"not-the,*,source-etag"',
  ])('copies when if-none-match %s does not match the source', async (ifNoneMatch) => {
    await expectCopied({ ifNoneMatch })
  })

  it('copies when if-none-match does not match even if the source was not modified after if-modified-since', async () => {
    await expectCopied({
      ifNoneMatch: '"not-the-source-etag"',
      ifModifiedSince: new Date(sourceLastModified.getTime() + 60_000),
    })
  })

  it('rejects the copy when if-modified-since equals the last-modified second', async () => {
    await expectPreconditionFailed({ ifModifiedSince: new Date('2026-01-01T00:00:00.000Z') })
  })

  it('copies when if-unmodified-since equals the last-modified second', async () => {
    await expectCopied({ ifUnmodifiedSince: new Date('2026-01-01T00:00:00.000Z') })
  })

  it('rejects the copy when if-match is empty', async () => {
    await expectPreconditionFailed({ ifMatch: '' })
  })

  it('copies when if-none-match is empty even if the source was not modified after if-modified-since', async () => {
    await expectCopied({
      ifNoneMatch: '',
      ifModifiedSince: new Date(sourceLastModified.getTime() + 60_000),
    })
  })
})
