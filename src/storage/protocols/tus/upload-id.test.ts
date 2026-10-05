vi.hoisted(() => {
  process.env.TUS_USE_FILE_VERSION_SEPARATOR = 'true'
})

import { describe, expect, it, vi } from 'vitest'
import { UploadId } from './upload-id'

describe('UploadId with TUS_USE_FILE_VERSION_SEPARATOR', () => {
  it('keeps the folders of the object name', () => {
    const uploadId = UploadId.fromString('tenant/bucket/folder/sub/cat.png-$v-version-id')

    expect(uploadId.tenant).toBe('tenant')
    expect(uploadId.bucket).toBe('bucket')
    expect(uploadId.objectName).toBe('folder/sub/cat.png')
    expect(uploadId.version).toBe('version-id')
  })

  it('round-trips an id created for a nested object', () => {
    const id = new UploadId({
      tenant: 'tenant',
      bucket: 'bucket',
      objectName: 'folder/cat.png',
      version: 'version-id',
    }).toString()

    expect(id).toBe('tenant/bucket/folder/cat.png-$v-version-id')

    const parsed = UploadId.fromString(id)
    expect(parsed.objectName).toBe('folder/cat.png')
    expect(parsed.version).toBe('version-id')
  })

  it('parses an object at the bucket root', () => {
    const uploadId = UploadId.fromString('tenant/bucket/cat.png-$v-version-id')

    expect(uploadId.objectName).toBe('cat.png')
    expect(uploadId.version).toBe('version-id')
  })

  it.each([
    'report-$v-2026.txt',
    'folder/report-$v-2026.txt',
    'report-$v-one-$v-two.txt',
    '-$v-report.txt',
    'report-$v-',
    'folder-$v-one/report.txt',
  ])('round-trips an object containing the version separator: %s', (objectName) => {
    const original = new UploadId({
      tenant: 'tenant',
      bucket: 'bucket',
      objectName,
      version: 'version-id',
    })

    expect(UploadId.fromString(original.toString())).toEqual(original)
  })

  it('rejects a missing version after an object containing the separator', () => {
    expect(() => UploadId.fromString('tenant/bucket/report-$v-2026.txt-$v-')).toThrow(
      'Version not provided'
    )
  })

  it('rejects an id without the version separator', () => {
    expect(() => UploadId.fromString('tenant/bucket/report.txt')).toThrow('Object name is invalid')
  })
})
