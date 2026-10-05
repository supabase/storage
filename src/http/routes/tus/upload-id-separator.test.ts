vi.hoisted(() => {
  process.env.TUS_USE_FILE_VERSION_SEPARATOR = 'true'
})

import { EventEmitter } from 'node:events'
import type { ServerResponse } from 'node:http'
import type { Database } from '@storage/database'
import type { DataStore } from '@tus/server'
import { type MultiPartRequest, namingFunction, onIncomingRequest } from './lifecycle'

describe('TUS authorization with the file version separator', () => {
  it.each([
    'report-$v-2026.txt',
    'folder/report-$v-one-$v-two.txt',
  ])('checks permission for the requested object: %s', async (objectName) => {
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
