import fs from 'fs-extra'
import os from 'os'
import path from 'path'
import { Readable } from 'stream'
import * as xattr from 'fs-xattr'
import { withOptionalVersion } from '../storage/backend/adapter'
import { FileBackend } from '../storage/backend/file'
import { getConfig } from '../config'

jest.mock('fs-xattr', () => ({
  set: jest.fn(() => Promise.resolve()),
  get: jest.fn(() => Promise.resolve(undefined)),
}))

describe('FileBackend xattr metadata', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('uses a distinct linux xattr key for etag', async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
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
      await fs.remove(tmpDir)
    }
  })

  it('reads linux etag xattr during multipart completion', async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storage-file-backend-'))
    const originalPlatformDescriptor = Object.getOwnPropertyDescriptor(process, 'platform')
    const originalStoragePath = process.env.STORAGE_FILE_BACKEND_PATH
    const originalFilePath = process.env.FILE_STORAGE_BACKEND_PATH
    let uploadSpy: jest.SpyInstance | undefined

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
      await fs.ensureDir(partDir)
      await fs.writeFile(partPath, 'hello')

      const xattrGet = xattr.get as jest.Mock
      xattrGet.mockImplementation((_file: string, attribute: string) => {
        if (attribute === 'user.supabase.etag') {
          return Promise.resolve(Buffer.from('part-etag'))
        }
        return Promise.resolve(undefined)
      })

      uploadSpy = jest
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
      await fs.remove(tmpDir)
    }
  })
})
