import { getEventListeners } from 'node:events'
import fs, { type ReadStream } from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { PassThrough, Readable } from 'node:stream'
import type { Part } from '@aws-sdk/client-s3'
import type { MetadataValue } from '@tus/s3-store'
import { Upload } from '@tus/server'
import { PartUploadSemaphore } from './part-upload-semaphore'
import { S3Store } from './s3-store'
import { TusChunkSplitter } from './splitter-workaround'

class TestStore extends S3Store {
  constructor(concurrency = 1) {
    super({
      partSize: 4,
      minPartSize: 4,
      maxConcurrentPartUploads: concurrency,
      s3ClientConfig: {
        bucket: 'test',
        region: 'us-east-1',
        credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      },
    })
  }

  semaphore() {
    return this.chunkSemaphore
  }

  upload(input: Readable, size = 100, id = 'upload-id') {
    return this.uploadParts(
      {
        file: new Upload({ id, size, offset: 0 }),
        'upload-id': 'multipart-id',
        'tus-version': '1.0.0',
      },
      input,
      1,
      0
    )
  }
}

class MultiPartStore extends TestStore {
  readonly calls: PromiseWithResolvers<string>[] = []

  protected override async uploadPart(): Promise<string> {
    const call = Promise.withResolvers<string>()
    this.calls.push(call)
    return call.promise
  }
}

const tick = () => new Promise<void>((resolve) => setImmediate(resolve))

class CollectingStore extends TestStore {
  parts: { number: number; body: string }[] = []
  incomplete: string | undefined

  protected override async uploadPart(
    _metadata: MetadataValue,
    source: ReadStream | Readable | Buffer,
    number: number
  ): Promise<string> {
    const chunks: Buffer[] = []
    for await (const chunk of source as Readable) chunks.push(Buffer.from(chunk))
    this.parts.push({ number, body: Buffer.concat(chunks).toString() })
    return 'etag'
  }

  protected override async uploadIncompletePart(_id: string, source: ReadStream): Promise<string> {
    const chunks: Buffer[] = []
    for await (const chunk of source) chunks.push(Buffer.from(chunk))
    this.incomplete = Buffer.concat(chunks).toString()
    return 'etag'
  }
}

class ConcurrentStore extends TestStore {
  active = 0
  maximum = 0

  constructor() {
    super(2)
  }

  protected override async uploadPart(
    metadata: MetadataValue,
    source: ReadStream | Readable | Buffer
  ): Promise<string> {
    this.active++
    this.maximum = Math.max(this.maximum, this.active)
    try {
      for await (const _chunk of source as Readable) {
        // Consume the actual part file as the S3 SDK would.
      }
      await new Promise<void>((resolve) => setImmediate(resolve))
      if (metadata.file.id.startsWith('fail-')) throw new Error('part failed')
      return 'etag'
    } finally {
      this.active--
    }
  }
}

describe('TUS S3 splitter workaround', () => {
  let directory: string
  const openedHandles: Awaited<ReturnType<typeof fsp.open>>[] = []

  beforeEach(async () => {
    directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'tus-workaround-test-'))
    vi.spyOn(os, 'tmpdir').mockReturnValue(directory)
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    for (const handle of openedHandles.splice(0)) await handle.close().catch(() => {})
    await fsp.rm(directory, { recursive: true, force: true })
  })

  it('does not leak a late permit or create a file after an aborted wait', async () => {
    const store = new TestStore()
    const semaphore = store.semaphore()
    const held = await semaphore.acquire()
    const acquire = vi.spyOn(semaphore, 'acquire')
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    source.write(Buffer.from('abc'))
    await vi.waitFor(() => expect(acquire).toHaveBeenCalledTimes(1))
    source.destroy(new Error('aborted'))
    await rejected
    await expect(acquire.mock.results[0].value).rejects.toThrow('destroyed')
    await held.release()

    const available = await semaphore.acquire()
    await available.release()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('closes and removes a file whose open completes after the source aborts', async () => {
    const opened = Promise.withResolvers<void>()
    const resume = Promise.withResolvers<void>()
    const realOpen = fsp.open.bind(fsp)
    const emitEvent = vi.spyOn(TusChunkSplitter.prototype, 'emitEvent')
    vi.spyOn(fsp, 'open').mockImplementation(async (...args) => {
      const handle = await realOpen(...args)
      openedHandles.push(handle)
      opened.resolve()
      await resume.promise
      return handle
    })
    const store = new TestStore()
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    source.write(Buffer.from('abc'))
    await opened.promise
    source.destroy(new Error('aborted'))
    await rejected
    resume.resolve()

    await vi.waitFor(async () => {
      expect(openedHandles[0].fd).toBe(-1)
      expect(await fsp.readdir(directory)).toEqual([])
    })
    expect(emitEvent.mock.calls.some(([event]) => event === 'chunkStarted')).toBe(false)
    await (await store.semaphore().acquire()).release()
  })

  it('returns capacity when a queued permit is granted before the source aborts', async () => {
    const store = new TestStore()
    const semaphore = store.semaphore()
    const held = await semaphore.acquire()
    const acquire = vi.spyOn(semaphore, 'acquire')
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    source.write(Buffer.from('abc'))
    await vi.waitFor(() => expect(acquire).toHaveBeenCalledTimes(1))
    const releasing = held.release()
    source.destroy(new Error('aborted'))
    await releasing
    await rejected
    await (await semaphore.acquire()).release()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('cleans up an incomplete chunk when the source closes without an error', async () => {
    const store = new TestStore()
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toMatchObject({ code: 'ERR_STREAM_PREMATURE_CLOSE' })
    source.write(Buffer.from('abc'))
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toHaveLength(1))
    source.destroy()
    await rejected
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
    await (await store.semaphore().acquire()).release()
  })

  it('does not release the previous part permit while its upload is in flight', async () => {
    const store = new MultiPartStore()
    const semaphore = store.semaphore()
    const acquire = vi.spyOn(semaphore, 'acquire')
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    source.write(Buffer.from('12345'))
    await vi.waitFor(() => expect(store.calls).toHaveLength(1))
    await vi.waitFor(() => expect(acquire).toHaveBeenCalledTimes(2))
    source.destroy(new Error('aborted'))
    await new Promise<void>((resolve) => setImmediate(resolve))

    let nextAcquired = false
    const next = semaphore.acquire().then((permit) => {
      nextAcquired = true
      return permit
    })
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(nextAcquired).toBe(false)
    store.calls[0].resolve('etag')
    await rejected
    await (await next).release()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('keeps a handed-off chunk until its delayed reader finishes after an abort', async () => {
    const opening = Promise.withResolvers<void>()
    const resume = Promise.withResolvers<void>()
    const realOpen = fs.open.bind(fs)
    const realCreateReadStream = fs.createReadStream.bind(fs)
    vi.spyOn(fs, 'createReadStream').mockImplementation((file) =>
      realCreateReadStream(file, {
        fs: {
          open: (...args: Parameters<typeof fs.open>) => {
            opening.resolve()
            resume.promise.then(() => realOpen(...args))
          },
          read: fs.read,
          close: fs.close,
        },
      })
    )
    const realRemove = fsp.rm.bind(fsp)
    vi.spyOn(fsp, 'rm').mockImplementation(async (file, options) => {
      if (options?.force) {
        fs.rmSync(file, options)
      } else {
        await realRemove(file, options)
      }
    })
    const originalNewChunk = TusChunkSplitter.prototype._newChunk
    let splitter: TusChunkSplitter | undefined
    vi.spyOn(TusChunkSplitter.prototype, '_newChunk').mockImplementation(function (
      this: TusChunkSplitter
    ) {
      splitter = this
      return originalNewChunk.call(this)
    })

    const store = new CollectingStore()
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    try {
      source.write(Buffer.from('abcde'))
      await opening.promise
      expect(splitter).toBeDefined()
      const closed = new Promise<void>((resolve) => splitter?.once('close', resolve))
      source.destroy(new Error('aborted'))
      await closed
      await tick()
      resume.resolve()
      await rejected

      expect(store.parts).toEqual([{ number: 1, body: 'abcd' }])
      await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
    } finally {
      resume.resolve()
      source.destroy()
    }
  })

  it('releases capacity when a part fails while the next chunk waits', async () => {
    const store = new MultiPartStore()
    const readers = vi.spyOn(fs, 'createReadStream')
    const semaphore = store.semaphore()
    const acquire = vi.spyOn(semaphore, 'acquire')
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('part failed')
    source.write(Buffer.from('12345'))
    await vi.waitFor(() => expect(store.calls).toHaveLength(1))
    await vi.waitFor(() => expect(acquire).toHaveBeenCalledTimes(2))
    store.calls[0].reject(new Error('part failed'))
    await rejected
    expect(readers).toHaveBeenCalledTimes(1)
    expect(readers.mock.results[0].value.destroyed).toBe(true)
    await vi.waitFor(() => expect(readers.mock.results[0].value.closed).toBe(true))
    const available = await semaphore.acquire()
    let nextAcquired = false
    const next = semaphore.acquire().then((permit) => {
      nextAcquired = true
      return permit
    })
    await tick()
    expect(nextAcquired).toBe(false)
    await available.release()
    await (await next).release()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('preserves part ordering, bytes and the returned count on success', async () => {
    const store = new CollectingStore()
    expect(await store.upload(Readable.from([Buffer.from('abcdefghijkl')]), 12)).toBe(12)
    expect(store.parts.sort((a, b) => a.number - b.number)).toEqual([
      { number: 1, body: 'abcd' },
      { number: 2, body: 'efgh' },
      { number: 3, body: 'ijkl' },
    ])
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('preserves incomplete-part uploads and empty input', async () => {
    const store = new CollectingStore()
    expect(await store.upload(Readable.from([Buffer.from('abc')]), 12)).toBe(3)
    expect(store.incomplete).toBe('abc')
    expect(store.parts).toEqual([])
    expect(await store.upload(Readable.from([]), 12)).toBe(0)
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('uploads a short final part as a multipart part', async () => {
    const store = new CollectingStore()
    expect(await store.upload(Readable.from([Buffer.from('abc')]), 3)).toBe(3)
    expect(store.parts).toEqual([{ number: 1, body: 'abc' }])
    expect(store.incomplete).toBeUndefined()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('rejects a pre-aborted semaphore acquisition without taking capacity', async () => {
    const semaphore = new PartUploadSemaphore(1)
    const controller = new AbortController()
    const reason = new Error('already aborted')
    controller.abort(reason)
    await expect(semaphore.acquire(controller.signal)).rejects.toBe(reason)
    await (await semaphore.acquire()).release()
  })

  it('removes cancelled waiters while preserving FIFO order and listeners', async () => {
    const semaphore = new PartUploadSemaphore(1)
    const held = await semaphore.acquire()
    const firstSignal = new AbortController()
    const cancelledSignal = new AbortController()
    const lastSignal = new AbortController()
    const order: number[] = []
    const first = semaphore.acquire(firstSignal.signal).then((permit) => {
      order.push(1)
      return permit
    })
    const cancelled = semaphore.acquire(cancelledSignal.signal)
    const rejected = expect(cancelled).rejects.toThrow('cancelled')
    const last = semaphore.acquire(lastSignal.signal).then((permit) => {
      order.push(3)
      return permit
    })
    cancelledSignal.abort(new Error('cancelled'))
    await rejected
    await held.release()
    await (await first).release()
    await (await last).release()
    expect(order).toEqual([1, 3])
    for (const controller of [firstSignal, cancelledSignal, lastSignal]) {
      expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0)
    }
  })

  it('enforces the shared part limit across successful and failed uploads', async () => {
    const store = new ConcurrentStore()
    const uploads = Array.from({ length: 24 }, (_, index) =>
      store.upload(
        Readable.from([Buffer.from('abcdefghijkl')]),
        12,
        index % 4 === 0 ? `fail-${index}` : `ok-${index}`
      )
    )
    const results = await Promise.allSettled(uploads)
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(18)
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(6)
    expect(store.maximum).toBe(2)
    expect(store.active).toBe(0)
    const permits = await Promise.all([store.semaphore().acquire(), store.semaphore().acquire()])
    await Promise.all(permits.map((permit) => permit.release()))
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  // A permit can be granted after this splitter has already been destroyed.
  it('returns a permit granted when two uploads abort in the same tick', async () => {
    const store = new TestStore(1)
    const semaphore = store.semaphore()
    const acquire = vi.spyOn(semaphore, 'acquire')
    const a = new PassThrough()
    const b = new PassThrough()
    const ra = expect(store.upload(a, 100, 'a')).rejects.toThrow('aborted')
    a.write(Buffer.from('abc'))
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toHaveLength(1))
    const rb = expect(store.upload(b, 100, 'b')).rejects.toThrow('aborted')
    b.write(Buffer.from('abc'))
    await vi.waitFor(() => expect(acquire).toHaveBeenCalledTimes(2))
    a.destroy(new Error('aborted'))
    b.destroy(new Error('aborted'))
    await ra
    await rb
    await (await semaphore.acquire()).release()
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  // The descriptor closes before the async close operation finishes while the
  // pipeline catch block is still removing the pending chunk.
  it('preserves the abort error when chunk close completes during cleanup', async () => {
    const gate = Promise.withResolvers<void>()
    const closing = Promise.withResolvers<void>()
    const rmGate = Promise.withResolvers<void>()
    const removing = Promise.withResolvers<void>()
    const realOpen = fsp.open.bind(fsp)
    const realRm = fsp.rm.bind(fsp)
    let opens = 0
    vi.spyOn(fsp, 'open').mockImplementation(async (...args) => {
      const handle = await realOpen(...args)
      if (++opens === 2) {
        const close = handle.close
        let first = true
        handle.close = async () => {
          const closed = close()
          if (first) {
            first = false
            closing.resolve()
            await gate.promise
          }
          return closed
        }
      }
      return handle
    })
    vi.spyOn(fsp, 'rm').mockImplementation(async (...args) => {
      if (args[1]?.force) {
        removing.resolve()
        await rmGate.promise
      }
      return realRm(...args)
    })
    const store = new MultiPartStore(2)
    const source = new PassThrough()
    const upload = store.upload(source)
    const rejected = expect(upload).rejects.toThrow('aborted')
    source.write(Buffer.from('abcdefghi'))
    await closing.promise
    store.calls[0].resolve('etag')
    source.destroy(new Error('aborted'))
    await removing.promise
    gate.resolve()
    rmGate.resolve()
    await rejected
    expect(store.calls).toHaveLength(1)
  })

  it('handles a part rejection before the pipeline settles', async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    const destroyStarted = Promise.withResolvers<void>()
    const releaseDestroy = Promise.withResolvers<void>()
    try {
      const store = new MultiPartStore(1)
      const source = new Readable({
        read() {},
        destroy(error, callback) {
          destroyStarted.resolve()
          releaseDestroy.promise.then(() => callback(error))
        },
      })
      const upload = store.upload(source)
      const rejected = expect(upload).rejects.toThrow('part failed')
      source.push(Buffer.from('abcde'))
      await vi.waitFor(() => expect(store.calls).toHaveLength(1))
      store.calls[0].reject(new Error('part failed'))
      await destroyStarted.promise
      await tick()
      expect(unhandled).toEqual([])
      releaseDestroy.resolve()
      await rejected
    } finally {
      releaseDestroy.resolve()
      process.off('unhandledRejection', onUnhandled)
    }
  })

  it('waits for in-flight parts after another part fails', async () => {
    const store = new MultiPartStore(2)
    const source = new PassThrough()
    let settled = false
    const upload = store.upload(source).finally(() => {
      settled = true
    })
    const rejected = expect(upload).rejects.toThrow('part failed')
    source.end(Buffer.from('abcdefgh'))
    await vi.waitFor(() => expect(store.calls).toHaveLength(2))
    store.calls[0].reject(new Error('part failed'))
    for (let i = 0; i < 5; i++) await tick()
    expect(settled).toBe(false)
    store.calls[1].resolve('etag')
    await rejected
  })
})

// A PATCH that resumes after an earlier one left an incomplete part (< minPartSize) in S3.
class ResumeStore extends CollectingStore {
  // Held open to simulate the S3 round trips write() makes before it reads the request body.
  readonly listParts = Promise.withResolvers<void>()
  readonly finished: string[] = []
  failParts = false
  // When set, the incomplete part reader stays open until endReader() is called.
  holdReaderOpen = false
  endReader = () => {}

  constructor(
    private readonly directory: string,
    private readonly incompletePart: string,
    private readonly size: number
  ) {
    super()
  }

  protected override async getMetadata(id: string): Promise<MetadataValue> {
    return {
      file: new Upload({ id, size: this.size, offset: 0 }),
      'upload-id': 'multipart-id',
      'tus-version': '1.0.0',
    }
  }

  protected override async retrieveParts(): Promise<Part[]> {
    await this.listParts.promise
    return []
  }

  protected override async downloadIncompletePart(id: string) {
    const filePath = path.join(this.directory, 'tus-s3-incomplete-part-test')
    await fsp.writeFile(filePath, this.incompletePart)
    return {
      size: this.incompletePart.length,
      path: filePath,
      createReader: () => {
        if (this.holdReaderOpen) {
          const reader = new PassThrough()
          reader.write(this.incompletePart)
          fs.rmSync(filePath, { force: true })
          this.endReader = () => reader.end()
          return reader as unknown as ReadStream
        }
        const reader = fs.createReadStream(filePath)
        reader.once('end', () => fs.rm(filePath, { force: true }, () => {}))
        reader.once('error', () => fs.rm(filePath, { force: true }, () => {}))
        return reader
      },
    }
  }

  protected override async deleteIncompletePart(): Promise<void> {}

  protected override async uploadPart(
    metadata: MetadataValue,
    source: ReadStream | Readable | Buffer,
    number: number
  ): Promise<string> {
    if (this.failParts) {
      for await (const _chunk of source as Readable) {
        // Consume the part file as the S3 SDK would before failing.
      }
      throw new Error('part failed')
    }
    return super.uploadPart(metadata, source, number)
  }

  protected override async finishMultipartUpload(): Promise<string> {
    this.finished.push('finishMultipartUpload')
    return 'location'
  }

  protected override async completeMetadata(): Promise<void> {
    this.finished.push('completeMetadata')
  }

  protected override async clearCache(): Promise<void> {
    this.finished.push('clearCache')
  }
}

describe('TUS S3 write workaround', () => {
  let directory: string
  const openedHandles: Awaited<ReturnType<typeof fsp.open>>[] = []

  beforeEach(async () => {
    directory = await fsp.mkdtemp(path.join(os.tmpdir(), 'tus-write-workaround-test-'))
    vi.spyOn(os, 'tmpdir').mockReturnValue(directory)
    const realOpen = fsp.open.bind(fsp)
    vi.spyOn(fsp, 'open').mockImplementation(async (...args) => {
      const handle = await realOpen(...args)
      openedHandles.push(handle)
      return handle
    })
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    for (const handle of openedHandles.splice(0)) await handle.close().catch(() => {})
    await fsp.rm(directory, { recursive: true, force: true })
  })

  it('prepends the incomplete part and finishes the upload', async () => {
    const store = new ResumeStore(directory, 'abc', 10)
    store.listParts.resolve()
    const source = Readable.from([Buffer.from('defghij')])

    await expect(store.write(source, 'upload-id', 3)).resolves.toBe(10)

    expect(store.parts).toEqual([
      { number: 1, body: 'abcd' },
      { number: 2, body: 'efgh' },
      { number: 3, body: 'ij' },
    ])
    expect(store.finished).toEqual(['finishMultipartUpload', 'completeMetadata', 'clearCache'])
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
  })

  it('fails instead of stalling when the request stream dies before it is joined', async () => {
    const store = new ResumeStore(directory, 'abc', 100)
    store.holdReaderOpen = true
    const source = new PassThrough()
    // In production the outer TUS pipeline is the listener that observes this error.
    source.on('error', () => {})
    const write = store.write(source, 'upload-id', 3)
    const rejected = expect(write).rejects.toThrow('Maximum size exceeded')

    // e.g. the StreamLimiter rejects the body while write() is still talking to S3
    source.destroy(new Error('Maximum size exceeded'))
    await tick()
    store.listParts.resolve()
    // The incomplete part reader never ends: the dead request stream alone must fail the write.
    await rejected

    // Any chunk must be closed explicitly (not left for GC) and removed, and its permit returned.
    await vi.waitFor(async () => {
      for (const handle of openedHandles) expect(handle.fd).toBe(-1)
      expect(await fsp.readdir(directory)).toEqual([])
    })
    await (await store.semaphore().acquire()).release()
  })

  it('fails right away when the request stream dies while the incomplete part is read', async () => {
    const store = new ResumeStore(directory, 'abc', 100)
    store.holdReaderOpen = true
    store.listParts.resolve()
    const source = new PassThrough()
    source.on('error', () => {})
    const write = store.write(source, 'upload-id', 3)
    const rejected = expect(write).rejects.toThrow('Maximum size exceeded')

    // The incomplete part is still streaming (e.g. waiting on a part permit) when the body fails.
    await vi.waitFor(() => expect(openedHandles).toHaveLength(1))
    source.destroy(new Error('Maximum size exceeded'))
    await rejected

    await vi.waitFor(async () => {
      expect(openedHandles[0].fd).toBe(-1)
      expect(await fsp.readdir(directory)).toEqual([])
    })
    await (await store.semaphore().acquire()).release()
  })

  it('destroys the request stream with the part upload error', async () => {
    const store = new ResumeStore(directory, 'abc', 100)
    store.failParts = true
    store.listParts.resolve()
    const source = new PassThrough()
    const sourceError = new Promise<Error>((resolve) => source.once('error', resolve))
    const write = store.write(source, 'upload-id', 3)
    const rejected = expect(write).rejects.toThrow('part failed')

    source.write(Buffer.from('defgh'))
    await rejected

    // The outer TUS pipeline reports the request stream's error, so it must stay the S3 error.
    await expect(sourceError).resolves.toMatchObject({ message: 'part failed' })
    await vi.waitFor(async () => expect(await fsp.readdir(directory)).toEqual([]))
    await (await store.semaphore().acquire()).release()
  })
})
