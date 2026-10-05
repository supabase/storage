import * as fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'
import { text } from 'node:stream/consumers'
import { removePath } from '@internal/fs'
import { vi } from 'vitest'
import { getConfig } from '../../config'
import { FileBackend } from './file'

vi.mock('fs-xattr', () => ({
  setAttributeSync: vi.fn(),
  getAttributeSync: vi.fn(),
  removeAttributeSync: vi.fn(),
}))

describe('FileBackend byte range reads', () => {
  let tmpDir: string
  let backend: FileBackend

  beforeEach(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'storage-file-range-'))
    vi.stubEnv('STORAGE_FILE_BACKEND_PATH', tmpDir)
    getConfig({ reload: true })
    backend = new FileBackend()
    await backend.uploadObject(
      'bucket',
      'object.txt',
      'v1',
      Readable.from('0123456789'),
      'text/plain',
      'no-cache'
    )
  })

  afterEach(async () => {
    vi.unstubAllEnvs()
    getConfig({ reload: true })
    await removePath(tmpDir)
  })

  it.each([
    ['bytes=2-5', '2345', 'bytes 2-5/10'],
    ['Bytes=2-5', '2345', 'bytes 2-5/10'],
    ['BYTES=7-', '789', 'bytes 7-9/10'],
    ['bYtEs=-5', '56789', 'bytes 5-9/10'],
  ])('reads %s with a case-insensitive range unit', async (range, expected, contentRange) => {
    const result = await backend.getObject('bucket', 'object.txt', 'v1', {
      range,
    })

    expect(result.httpStatusCode).toBe(206)
    expect(result.metadata).toMatchObject({
      contentRange,
      contentLength: expected.length,
      size: expected.length,
    })
    await expect(text(result.body as Readable)).resolves.toBe(expected)
  })
})
