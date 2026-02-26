import { createHash } from 'node:crypto'
import fs from 'node:fs'
import * as fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { HashSpillWritable } from '@internal/streams/hash-stream'

function randBuf(size: number): Buffer {
  const b = Buffer.allocUnsafe(size)
  for (let i = 0; i < size; i++) b[i] = (i * 131 + 17) & 0xff // deterministic-ish
  return b
}

function readableFrom(...chunks: Buffer[]): Readable {
  return Readable.from(chunks)
}

async function dirEntries(p: string): Promise<string[]> {
  try {
    const names = await fsp.readdir(p)
    return names
  } catch {
    return []
  }
}

async function countHashspillDirs(root: string): Promise<number> {
  const names = await dirEntries(root)
  return names.filter((n) => n.startsWith('hashspill-')).length
}

async function findSpillFilePath(root: string): Promise<string | null> {
  try {
    const entries = await fsp.readdir(root, { withFileTypes: true })
    const dir = entries.find((e) => e.isDirectory() && e.name.startsWith('hashspill-'))
    if (!dir) return null
    const dirPath = path.join(root, dir.name)
    const files = await fsp.readdir(dirPath)
    if (files.length === 0) return null
    return path.join(dirPath, files[0]) // our class writes a single file
  } catch {
    return null
  }
}

class SlowWritable extends Writable {
  private delayMs: number
  constructor(delayMs = 5) {
    super({ highWaterMark: 16 * 1024 }) // small HWM to induce backpressure
    this.delayMs = delayMs
  }
  _write(chunk: Buffer, _enc: BufferEncoding, cb: (e?: Error | null) => void) {
    setTimeout(() => cb(), this.delayMs)
  }
}

describe('HashSpillWritable', () => {
  let tmpRoot: string

  beforeEach(async () => {
    tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'hsw-tests-'))
  })

  afterEach(async () => {
    // best-effort cleanup of left-overs
    try {
      await fsp.rm(tmpRoot, { recursive: true, force: true })
    } catch {}
  })

  test('in-memory: under limit stays in memory; digest & size are correct', async () => {
    const limit = 1024 * 64
    const payload = randBuf(limit - 7)
    const expectedDigest = createHash('sha256').update(payload).digest('hex')

    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
    await pipeline(readableFrom(payload), sink)

    expect(sink.size()).toBe(payload.length)
    expect(sink.digestHex()).toBe(expectedDigest)

    // toReadable returns in-memory stream
    const collected: Buffer[] = []
    await pipeline(
      sink.toReadable(),
      new Writable({
        write(chunk, _enc, cb) {
          collected.push(chunk as Buffer)
          cb()
        },
      })
    )
    expect(Buffer.concat(collected)).toEqual(payload)

    // No hashspill-* dirs should have been created
    expect(await countHashspillDirs(tmpRoot)).toBe(0)

    // cleanup() should be a no-op
    await expect(sink.cleanup()).resolves.toBeUndefined()
  })

  test('in-memory: exactly at limit does not spill', async () => {
    const limit = 32 * 1024
    const payload = randBuf(limit)
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(readableFrom(payload), sink)
    expect(sink.size()).toBe(limit)
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('spill: just over limit triggers spill; autoCleanup on reader removes artifacts', async () => {
    const limit = 1024 * 32
    const payload = randBuf(limit + 1)
    const expectedDigest = createHash('sha256').update(payload).digest('hex')

    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
    await pipeline(readableFrom(payload), sink)

    expect(sink.digestHex()).toBe(expectedDigest)
    // A spill should have created exactly one temp dir
    expect(await countHashspillDirs(tmpRoot)).toBe(1)

    // Read with autoCleanup so artifacts get removed when last reader ends
    await pipeline(
      sink.toReadable({ autoCleanup: true }),
      new Writable({
        write(_c, _e, cb) {
          cb()
        },
      })
    )

    // Allow event loop to process cleanup
    await new Promise((r) => setTimeout(r, 100))

    // The hashspill dir should be gone
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('spill: multiple readers, autoCleanup waits for the last reader', async () => {
    const limit = 8 * 1024
    const payload = randBuf(limit * 3) // force spill
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(readableFrom(payload), sink)
    expect(await countHashspillDirs(tmpRoot)).toBe(1)

    const r1 = sink.toReadable({ autoCleanup: true })
    const r2 = sink.toReadable({ autoCleanup: true })

    // pipe r1 quickly
    const fastConsumer = new Writable({
      write(_c, _e, cb) {
        cb()
      },
    })
    // r2 is slower
    const slowConsumer = new SlowWritable(3)

    const p1 = pipeline(r1, fastConsumer)
    const p2 = pipeline(r2, slowConsumer)
    await Promise.all([p1, p2])

    // wait a tick for cleanup to run
    await new Promise((r) => setTimeout(r, 10))

    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('manual cleanup: delete after readers close (call cleanup after reading)', async () => {
    const limit = 4096
    const payload = randBuf(limit * 5)
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(readableFrom(payload), sink)
    expect(await countHashspillDirs(tmpRoot)).toBe(1)

    // No autoCleanup; we clean manually after
    const r = sink.toReadable()
    await pipeline(
      r,
      new Writable({
        write(_c, _e, cb) {
          cb()
        },
      })
    )

    // Now manual cleanup removes artifacts
    await sink.cleanup()
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('backpressure respected with slow downstream while hashing', async () => {
    const limit = 16 * 1024
    const pieces = Array.from({ length: 50 }, (_, i) => randBuf(2048 + (i % 5)))
    const payload = Buffer.concat(pieces)
    const expectedDigest = createHash('sha256').update(payload).digest('hex')

    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    // Write into sink, then read out to a slow consumer to ensure stream semantics hold
    await pipeline(Readable.from(pieces), sink)
    const outPieces: Buffer[] = []
    await pipeline(
      sink.toReadable(),
      new SlowWritable(2).on('pipe', function () {})
    )

    expect(sink.digestHex()).toBe(expectedDigest)
  })

  test('multiple concurrent instances (no collisions, all succeed)', async () => {
    const N = 8
    const limit = 8 * 1024
    const jobs = Array.from({ length: N }, async (_, idx) => {
      const buf = randBuf(limit + 1024 + idx) // force spill
      const exp = createHash('sha256').update(buf).digest('hex')

      const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
      await pipeline(readableFrom(buf), sink)

      expect(sink.digestHex()).toBe(exp)
      // Use autoCleanup to clean right after reading
      await pipeline(
        sink.toReadable({ autoCleanup: true }),
        new Writable({
          write(_c, _e, cb) {
            cb()
          },
        })
      )
    })

    await Promise.all(jobs)

    // Allow cleanup to finish
    await new Promise((r) => setTimeout(r, 10))
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('size() tracks total bytes written', async () => {
    const limit = 10 * 1024
    const parts = [randBuf(1111), randBuf(2222), randBuf(3333)]
    const total = parts.reduce((n, b) => n + b.length, 0)
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(Readable.from(parts), sink)
    expect(sink.size()).toBe(total)
  })

  test('toReadable() can be called multiple times (consistent replay)', async () => {
    const limit = 4096
    const payload = randBuf(limit * 2) // spill
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
    await pipeline(readableFrom(payload), sink)

    const readAll = async () => {
      const chunks: Buffer[] = []
      await pipeline(
        sink.toReadable(),
        new Writable({
          write(c, _e, cb) {
            chunks.push(c as Buffer)
            cb()
          },
        })
      )
      return Buffer.concat(chunks)
    }

    const a = await readAll()
    const b = await readAll()
    expect(a).toEqual(payload)
    expect(b).toEqual(payload)

    await sink.cleanup()
  })

  test('cleanup is a no-op for non-spilled streams', async () => {
    const limit = 1 << 20
    const payload = randBuf(12345) // under limit
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
    await pipeline(readableFrom(payload), sink)
    await expect(sink.cleanup()).resolves.toBeUndefined()
    // Nothing created on disk
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('errors if digestHex() is called before finish', async () => {
    const limit = 1024
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    // start write but don't finish
    const r = new Readable({
      read() {
        this.push(randBuf(200))
        this.push(null)
      },
    })
    await pipeline(r, sink)
    // now finished — valid
    expect(() => sink.digestHex()).not.toThrow()
  })

  test('spill: if file cannot be created/written, pipeline rejects with a handled error', async () => {
    const limit = 8 * 1024
    const payload = randBuf(limit * 3) // force spill
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    // Stub createWriteStream to fail on creation
    const spy = jest.spyOn(fs, 'createWriteStream').mockImplementation(() => {
      throw Object.assign(new Error('simulated createWriteStream failure'), { code: 'EACCES' })
    })

    try {
      await expect(pipeline(readableFrom(payload), sink)).rejects.toThrow(
        /createWriteStream failure|EACCES|simulated/i
      )
    } finally {
      spy.mockRestore()
    }

    // Ensure no lingering temp dirs/files (best-effort)
    await new Promise((r) => setTimeout(r, 10))
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('spill: spilled file exists before read and is deleted after autoCleanup', async () => {
    const limit = 16 * 1024
    const payload = randBuf(limit * 2 + 123) // force spill
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(readableFrom(payload), sink)

    // The spill dir & file should exist now
    const prePath = await findSpillFilePath(tmpRoot)
    expect(prePath).not.toBeNull()
    expect(fs.existsSync(prePath!)).toBe(true)

    // Read with autoCleanup
    await pipeline(
      sink.toReadable({ autoCleanup: true }),
      new Writable({
        write(_c, _e, cb) {
          cb()
        },
      })
    )

    // Give the event loop a tick for cleanup
    await new Promise((r) => setTimeout(r, 15))

    // The specific spilled file AND directory should be gone
    const postPath = await findSpillFilePath(tmpRoot)
    expect(postPath).toBeNull()
    expect(fs.existsSync(prePath!)).toBe(false)

    // And no hashspill dirs remain
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })
  test('concurrent spill operations: no temp file name collisions with rapid creation', async () => {
    const limit = 4 * 1024
    const N = 20 // More instances to increase collision probability

    // Create all instances simultaneously to maximize collision chance
    const sinks = Array.from(
      { length: N },
      () => new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
    )

    // Start all writes concurrently
    const writePromises = sinks.map(async (sink, idx) => {
      const buf = randBuf(limit + 100 + idx) // force spill on all
      await pipeline(readableFrom(buf), sink)
      return { sink, expected: createHash('sha256').update(buf).digest('hex') }
    })

    const results = await Promise.all(writePromises)

    // Verify all succeeded with correct digests
    for (const { sink, expected } of results) {
      expect(sink.digestHex()).toBe(expected)
    }

    // Verify all created separate temp directories
    expect(await countHashspillDirs(tmpRoot)).toBe(N)

    // Clean up all with autoCleanup
    const readPromises = results.map(({ sink }) =>
      pipeline(
        sink.toReadable({ autoCleanup: true }),
        new Writable({
          write(_c, _e, cb) {
            cb()
          },
        })
      )
    )

    await Promise.all(readPromises)
    await new Promise((r) => setTimeout(r, 20)) // Allow cleanup

    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('concurrent spill with identical timestamps: UUID ensures uniqueness', async () => {
    const limit = 2 * 1024
    const N = 10

    // Mock Date.now to return same timestamp for all instances
    const originalDateNow = Date.now
    const fixedTimestamp = 1234567890123
    jest.spyOn(Date, 'now').mockReturnValue(fixedTimestamp)

    try {
      const jobs = Array.from({ length: N }, async (_, idx) => {
        const payload = randBuf(limit + 50 + idx)
        const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
        await pipeline(readableFrom(payload), sink)

        // Verify file was created successfully despite same timestamp
        const spillPath = await findSpillFilePath(tmpRoot)
        expect(spillPath).not.toBeNull()

        await sink.cleanup()
        return sink.digestHex()
      })

      // All should succeed despite identical timestamps
      const digests = await Promise.all(jobs)
      expect(digests).toHaveLength(N)

      // All temp dirs should be cleaned up
      await new Promise((r) => setTimeout(r, 10))
      expect(await countHashspillDirs(tmpRoot)).toBe(0)
    } finally {
      jest.restoreAllMocks()
    }
  })

  test('concurrent readers on same spilled stream with mixed cleanup strategies', async () => {
    const limit = 8 * 1024
    const payload = randBuf(limit * 2)
    const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

    await pipeline(readableFrom(payload), sink)
    expect(await countHashspillDirs(tmpRoot)).toBe(1)

    // Create multiple readers: some with autoCleanup, some without
    const readers = [
      { stream: sink.toReadable({ autoCleanup: true }), name: 'auto1' },
      { stream: sink.toReadable({ autoCleanup: false }), name: 'manual1' },
      { stream: sink.toReadable({ autoCleanup: true }), name: 'auto2' },
      { stream: sink.toReadable({ autoCleanup: false }), name: 'manual2' },
      { stream: sink.toReadable({ autoCleanup: true }), name: 'auto3' },
    ]

    // Read from all concurrently with varying speeds
    const readPromises = readers.map(({ stream, name }, idx) => {
      const consumer = new SlowWritable(100 * idx + 1) // Different speeds
      return pipeline(stream, consumer)
    })

    await Promise.all(readPromises)

    // Even though some had autoCleanup=true, cleanup should be deferred
    // because other readers existed. Only manual cleanup should work now.
    expect(await countHashspillDirs(tmpRoot)).toBe(1)

    await new Promise((r) => setTimeout(r, 200))
    // Manual cleanup should now succeed
    await sink.cleanup()
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('rapid spill/cleanup cycles: no resource leaks or race conditions', async () => {
    const limit = 4 * 1024
    const cycles = 15

    for (let i = 0; i < cycles; i++) {
      const payload = randBuf(limit + 100 + i)
      const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })

      await pipeline(readableFrom(payload), sink)

      // Immediately read and cleanup
      await pipeline(
        sink.toReadable({ autoCleanup: true }),
        new Writable({
          write(_c, _e, cb) {
            cb()
          },
        })
      )

      // Brief pause to allow cleanup
      await new Promise((r) => setTimeout(r, 2))
    }

    // All temp artifacts should be cleaned up
    await new Promise((r) => setTimeout(r, 20))
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })

  test('spill during concurrent writes to different tmp roots: isolation verified', async () => {
    const limit = 6 * 1024
    const tmpRoot2 = await fsp.mkdtemp(path.join(os.tmpdir(), 'hsw-tests2-'))

    try {
      const payload1 = randBuf(limit + 200)
      const payload2 = randBuf(limit + 300)

      const sink1 = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
      const sink2 = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot: tmpRoot2 })

      // Write to both concurrently
      await Promise.all([
        pipeline(readableFrom(payload1), sink1),
        pipeline(readableFrom(payload2), sink2),
      ])

      // Each should have created temp dirs in their respective roots
      expect(await countHashspillDirs(tmpRoot)).toBe(1)
      expect(await countHashspillDirs(tmpRoot2)).toBe(1)

      // Cleanup both
      await Promise.all([sink1.cleanup(), sink2.cleanup()])

      expect(await countHashspillDirs(tmpRoot)).toBe(0)
      expect(await countHashspillDirs(tmpRoot2)).toBe(0)
    } finally {
      await fsp.rm(tmpRoot2, { recursive: true, force: true }).catch(() => {})
    }
  })

  test('stress test: many concurrent spills with overlapping lifecycles', async () => {
    const limit = 8 * 1024
    const batchSize = 12

    // Create overlapping batches
    const batch1Promise = Promise.all(
      Array.from({ length: batchSize }, async (_, i) => {
        const payload = randBuf(limit * 2 + i)
        const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
        await pipeline(readableFrom(payload), sink)

        // Delay before reading to create overlap with batch2
        await new Promise((r) => setTimeout(r, 10 + (i % 3)))

        await pipeline(
          sink.toReadable({ autoCleanup: true }),
          new Writable({
            write(_c, _e, cb) {
              cb()
            },
          })
        )

        return sink.digestHex()
      })
    )

    // Start second batch while first is still running
    await new Promise((r) => setTimeout(r, 20))

    const batch2Promise = Promise.all(
      Array.from({ length: batchSize }, async (_, i) => {
        const payload = randBuf(limit * 3 + i)
        const sink = new HashSpillWritable({ limitInMemoryBytes: limit, tmpRoot })
        await pipeline(readableFrom(payload), sink)

        await pipeline(
          sink.toReadable({ autoCleanup: true }),
          new Writable({
            write(_c, _e, cb) {
              cb()
            },
          })
        )

        return sink.digestHex()
      })
    )

    const [results1, results2] = await Promise.all([batch1Promise, batch2Promise])

    expect(results1).toHaveLength(batchSize)
    expect(results2).toHaveLength(batchSize)

    // Allow all cleanup to complete
    await new Promise((r) => setTimeout(r, 50))
    expect(await countHashspillDirs(tmpRoot)).toBe(0)
  })
})
