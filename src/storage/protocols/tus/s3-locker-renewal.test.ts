import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3'
import { vi } from 'vitest'
import { S3Locker } from './s3-locker'

describe('S3Locker renewal scheduling', () => {
  it('does not cancel a live lock when a slow renewal overlaps the next interval', async () => {
    vi.useFakeTimers()
    let lockBody = ''
    let etag = 'initial'
    let conditionalWrites = 0
    let firstRenewalInFlight = false
    let finishFirstRenewal!: () => void
    const firstRenewal = new Promise<void>((resolve) => {
      finishFirstRenewal = resolve
    })
    const send = vi.fn(async (command: unknown) => {
      if (command instanceof PutObjectCommand) {
        if (command.input.IfNoneMatch === '*') {
          lockBody = String(command.input.Body)
          return { ETag: etag }
        }

        conditionalWrites++
        if (conditionalWrites === 1) {
          firstRenewalInFlight = true
          await firstRenewal
          etag = 'renewed'
          lockBody = String(command.input.Body)
          firstRenewalInFlight = false
          return { ETag: etag }
        }
        if (firstRenewalInFlight) {
          await firstRenewal
          throw Object.assign(new Error('etag changed'), {
            name: 'PreconditionFailed',
            $metadata: { httpStatusCode: 412 },
          })
        }
        if (command.input.IfMatch !== etag) {
          throw Object.assign(new Error('etag changed'), {
            name: 'PreconditionFailed',
            $metadata: { httpStatusCode: 412 },
          })
        }
        etag = `renewed-${conditionalWrites}`
        lockBody = String(command.input.Body)
        return { ETag: etag }
      }

      if (command instanceof GetObjectCommand) {
        return {
          ETag: etag,
          Body: { transformToString: async () => lockBody },
        }
      }

      if (command instanceof DeleteObjectCommand) return {}
      throw new Error(`Unexpected command: ${String(command)}`)
    })
    const cancelReq = vi.fn()
    const locker = new S3Locker({
      s3Client: { send } as never,
      bucket: 'locks',
      notifier: { release: vi.fn(), onRelease: vi.fn(), unsubscribe: vi.fn() } as never,
      lockTtlMs: 1000,
      renewalIntervalMs: 10,
      logger: { log: vi.fn(), warn: vi.fn(), error: vi.fn() },
    })
    const lock = locker.newLock('upload-id')

    try {
      await lock.lock(new AbortController().signal, cancelReq)
      await vi.advanceTimersByTimeAsync(20)
      expect(conditionalWrites).toBe(1)

      finishFirstRenewal()
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      await vi.advanceTimersByTimeAsync(10)

      expect(conditionalWrites).toBe(2)
      expect(cancelReq).not.toHaveBeenCalled()
    } finally {
      await lock.unlock()
      vi.useRealTimers()
    }
  })
})
