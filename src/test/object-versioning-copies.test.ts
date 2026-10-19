import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { ObjectCreatedCopyEvent } from '../storage/events'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object versioning - copyObject', () => {
  const tHelper = useStorage()
  let bucketId: string
  let sourceName: string
  let destName: string

  beforeEach(async () => {
    bucketId = `versioning-copies-${randomUUID()}`
    sourceName = `copy-src-${randomUUID()}.txt`
    destName = `copy-dst-${randomUUID()}.txt`
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
  })

  afterEach(async () => {
    await withDeleteEnabled(tHelper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  async function upload(objectName: string, isUpsert: boolean, content: string) {
    const upload = await tHelper.uploader.upload({
      bucketId,
      objectName,
      uploadType: 'standard',
      isUpsert,
      file: {
        body: Readable.from(Buffer.from(content)),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })
    return upload.obj.version as string
  }

  it('ENABLED: copying onto a destination with existing history reports the full new size, nothing replaced', async () => {
    const content = 'source content'
    await upload(sourceName, false, content)
    await upload(destName, false, 'destination v1')
    await upload(destName, true, 'destination v2')

    const createdSpy = vi.spyOn(ObjectCreatedCopyEvent, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).copyObject({
        sourceKey: sourceName,
        destinationBucket: bucketId,
        destinationKey: destName,
        upsert: true,
        uploadType: 'standard',
      })

      // ENABLED never reuses a row in place - the destination's prior
      // current version is archived, not replaced, so there's nothing to net out.
      expect(createdSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          bytesChanged: content.length,
        })
      )
      const sent = createdSpy.mock.calls[0][0] as unknown as { replacedObject?: unknown }
      expect(sent.replacedObject).toBeUndefined()
    } finally {
      createdSpy.mockRestore()
    }
  })

  it('SUSPENDED: copying onto a destination overwrites its own null-version row in place', async () => {
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const content = 'new source content'
    await upload(sourceName, false, content)
    const overwrittenContent = 'overwritten destination content'
    await upload(destName, false, overwrittenContent)

    const createdSpy = vi.spyOn(ObjectCreatedCopyEvent, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).copyObject({
        sourceKey: sourceName,
        destinationBucket: bucketId,
        destinationKey: destName,
        upsert: true,
        uploadType: 'standard',
      })

      expect(createdSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          metadata: expect.objectContaining({ size: content.length }),
          bytesChanged: content.length - overwrittenContent.length,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: overwrittenContent.length }),
          }),
        })
      )
    } finally {
      createdSpy.mockRestore()
    }
  })
})
