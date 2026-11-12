import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3'
import type { Mock } from 'vitest'
import { vi } from 'vitest'
import { S3Locker } from './s3-locker'

function createS3LockHarness() {
  let currentBody: string | undefined
  let currentEtag: string | undefined
  let nextEtag = 0
  const send: Mock = vi.fn(async (command: unknown) => {
    if (command instanceof PutObjectCommand) {
      const { IfMatch, IfNoneMatch, Body } = command.input
      if (IfNoneMatch === '*' && currentBody !== undefined) {
        throw Object.assign(new Error('already exists'), {
          name: 'PreconditionFailed',
          $metadata: { httpStatusCode: 412 },
        })
      }
      if (IfMatch && IfMatch !== currentEtag) {
        throw Object.assign(new Error('etag changed'), {
          name: 'PreconditionFailed',
          $metadata: { httpStatusCode: 412 },
        })
      }
      currentBody = String(Body)
      currentEtag = `etag-${++nextEtag}`
      return { ETag: currentEtag }
    }

    if (command instanceof GetObjectCommand) {
      if (currentBody === undefined) {
        throw Object.assign(new Error('missing'), { name: 'NoSuchKey' })
      }
      return {
        ETag: currentEtag,
        Body: { transformToString: async () => currentBody },
      }
    }

    if (command instanceof DeleteObjectCommand) {
      currentBody = undefined
      currentEtag = undefined
      return {}
    }

    throw new Error(`Unexpected command: ${String(command)}`)
  })

  return {
    send,
    expireCurrentLock() {
      const metadata = JSON.parse(currentBody!) as { expiresAt: number }
      currentBody = JSON.stringify({ ...metadata, expiresAt: Date.now() - 1 })
      currentEtag = `etag-${++nextEtag}`
    },
    hasCurrentLock: () => currentBody !== undefined,
  }
}

describe('S3Locker lease ownership', () => {
  it('does not let an expired holder delete a successor lock when it unlocks', async () => {
    const s3 = createS3LockHarness()
    const notifier = {
      release: vi.fn(),
      onRelease: vi.fn(),
      unsubscribe: vi.fn(),
    }
    const locker = new S3Locker({
      s3Client: { send: s3.send } as never,
      bucket: 'locks',
      keyPrefix: 'tus/',
      notifier: notifier as never,
      lockTtlMs: 60_000,
      renewalIntervalMs: 30_000,
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
    })

    const first = locker.newLock('upload-id')
    const firstAbort = new AbortController()
    await first.lock(firstAbort.signal, vi.fn())
    s3.expireCurrentLock()

    const successor = locker.newLock('upload-id')
    await successor.lock(new AbortController().signal, vi.fn())
    expect(s3.hasCurrentLock()).toBe(true)

    await first.unlock()

    expect(s3.hasCurrentLock()).toBe(true)
    await successor.unlock()
  })
})
