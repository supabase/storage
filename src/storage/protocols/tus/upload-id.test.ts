vi.hoisted(() => {
  process.env.TUS_USE_FILE_VERSION_SEPARATOR = 'true'
})

import { EventEmitter } from 'node:events'
import type { ServerResponse } from 'node:http'
import type { Database } from '@storage/database'
import type { DataStore } from '@tus/server'
import { describe, expect, it, vi } from 'vitest'
import {
  type MultiPartRequest,
  namingFunction,
  onIncomingRequest,
} from '../../../http/routes/tus/lifecycle'
import { UploadId } from './upload-id'

describe('UploadId with TUS_USE_FILE_VERSION_SEPARATOR', () => {
  it.each([
    'cat.png',
    'folder/cat.png',
    'folder/sub/cat.png',
    'report-$v-2026.txt',
    'folder/report-$v-2026.txt',
    'report-$v-one-$v-two.txt',
    '-$v-report.txt',
    'report-$v-',
    'folder-$v-one/report.txt',
  ])('preserves the object name and version: %s', (objectName) => {
    const original = new UploadId({
      tenant: 'tenant',
      bucket: 'bucket',
      objectName,
      version: 'version-id',
    })

    const id = `tenant/bucket/${objectName}-$v-version-id`

    expect(original.toString()).toBe(id)
    expect(UploadId.fromString(id)).toEqual(original)
  })

  it.each([
    ['tenant/bucket/report-$v-2026.txt-$v-', 'Version not provided'],
    ['tenant/bucket/report.txt', 'Object name is invalid'],
  ])('rejects an invalid upload id: %s', (id, message) => {
    expect(() => UploadId.fromString(id)).toThrow(message)
  })
})

describe('TUS authorization with the file version separator', () => {
  it('checks permission for the full object name containing multiple separators', async () => {
    const objectName = 'folder/report-$v-one-$v-two.txt'
    const createObject = vi.fn().mockResolvedValue({})
    const request = {
      headers: { 'upload-length': '1' },
      method: 'POST',
      url: '/upload/resumable',
      upload: {
        tenantId: 'tenant',
        owner: 'owner',
        isUpsert: false,
        db: { dispose: vi.fn() },
        storage: {
          backend: {},
          location: {},
          db: {
            testPermission: (callback: (db: Pick<Database, 'createObject'>) => unknown) =>
              callback({ createObject }),
          },
        },
      },
    } as unknown as MultiPartRequest
    const response = new EventEmitter()
    const rawRequest = {
      method: 'POST',
      runtime: {
        name: 'node',
        node: { req: request, res: response as unknown as ServerResponse },
      },
    } as unknown as Parameters<typeof onIncomingRequest>[0]
    const id = namingFunction(rawRequest, { bucketName: 'bucket', objectName })

    await onIncomingRequest(rawRequest, id, {} as DataStore)

    expect(createObject).toHaveBeenCalledWith(
      expect.objectContaining({ bucket_id: 'bucket', name: objectName, owner: 'owner' })
    )
    expect(request.upload.resources).toEqual([`bucket/${objectName}`])
    response.emit('finish')
  })
})
