import * as fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'
import { encrypt } from '@internal/auth'
import { removePath } from '@internal/fs'
import { vi } from 'vitest'
import { getConfig } from '../../../config'
import { FileBackend, withOptionalVersion } from '../../backend'
import { TenantLocation } from '../../locator'
import { Storage } from '../../storage'
import { S3ProtocolHandler } from './s3-handler'

vi.mock('fs-xattr', () => ({
  setAttributeSync: vi.fn(),
  getAttributeSync: vi.fn(),
  removeAttributeSync: vi.fn(),
}))

describe('S3 multipart bucket size limits', () => {
  let tmpDir: string
  let handler: S3ProtocolHandler
  let uploadId: string
  let partPath: string
  let bucketLimit: number | null
  let multipart: { in_progress_size: number; upload_signature: string }

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-multipart-limit-'))
    vi.stubEnv('STORAGE_FILE_BACKEND_PATH', tmpDir)
    getConfig({ reload: true })
    const backend = new FileBackend()
    const { storageS3Bucket } = getConfig()
    const location = new TenantLocation(storageS3Bucket)
    const key = location.getKeyLocation({
      tenantId: 'tenant-id',
      bucketId: 'bucket',
      objectName: 'object.txt',
    })
    uploadId = (await backend.createMultiPartUpload(
      storageS3Bucket,
      key,
      'v1',
      'text/plain',
      'no-cache'
    )) as string
    partPath = path.join(
      tmpDir,
      'multiparts',
      uploadId,
      storageS3Bucket,
      withOptionalVersion(key, 'v1'),
      'part-1'
    )
    bucketLimit = 0
    multipart = { in_progress_size: 0, upload_signature: encrypt('progress:0') }

    const db = {
      tenantId: 'tenant-id',
      asSuperUser: vi.fn(),
      findBucketById: vi.fn(async () => ({ file_size_limit: bucketLimit })),
      findMultipartUpload: vi.fn(async () => ({
        ...multipart,
        bucket_id: 'bucket',
        key: 'object.txt',
        version: 'v1',
      })),
      withTransaction: vi.fn(),
      testPermission: vi.fn(),
      upsertObject: vi.fn(async () => ({})),
      updateMultipartUploadProgress: vi.fn(async (_id: string, size: number, signature: string) => {
        multipart.in_progress_size = size
        multipart.upload_signature = signature
      }),
      insertUploadPart: vi.fn(async () => ({})),
    }
    db.asSuperUser.mockReturnValue(db)
    db.withTransaction.mockImplementation((callback) => callback(db))
    db.testPermission.mockImplementation((callback) => callback(db))
    handler = new S3ProtocolHandler(new Storage(backend, db as never, location), 'tenant-id')
  })

  afterEach(async () => {
    vi.unstubAllEnvs()
    getConfig({ reload: true })
    await removePath(tmpDir)
  })

  const uploadPart = (body: string) =>
    handler.uploadPart(
      {
        Bucket: 'bucket',
        Key: 'object.txt',
        UploadId: uploadId,
        PartNumber: 1,
        ContentLength: Buffer.byteLength(body),
        Body: Readable.from(body),
      },
      {}
    )

  it('rejects a nonempty part for a zero-byte bucket without writing bytes or advancing progress', async () => {
    await expect(uploadPart('part-data')).rejects.toMatchObject({ code: 'EntityTooLarge' })
    await expect(fsp.stat(partPath)).rejects.toMatchObject({ code: 'ENOENT' })
    expect(multipart.in_progress_size).toBe(0)
  })

  it('allows an empty part for a zero-byte bucket', async () => {
    await expect(uploadPart('')).resolves.toHaveProperty('headers.etag')
    await expect(fsp.readFile(partPath, 'utf8')).resolves.toBe('')
    expect(multipart.in_progress_size).toBe(0)
  })

  it.each([9, null])('allows a part within bucket limit %s', async (limit) => {
    bucketLimit = limit
    await expect(uploadPart('part-data')).resolves.toHaveProperty('headers.etag')
    await expect(fsp.readFile(partPath, 'utf8')).resolves.toBe('part-data')
    expect(multipart.in_progress_size).toBe(9)
  })

  it('still rejects a part above a positive bucket limit', async () => {
    bucketLimit = 8
    await expect(uploadPart('part-data')).rejects.toMatchObject({ code: 'EntityTooLarge' })
    await expect(fsp.stat(partPath)).rejects.toMatchObject({ code: 'ENOENT' })
    expect(multipart.in_progress_size).toBe(0)
  })
})
