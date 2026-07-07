import nodeFs from 'fs'
import fs from 'fs/promises'
import os from 'os'
import path from 'path'
import { Readable, Writable } from 'stream'
import { vi } from 'vitest'
import { writeMultipartPprofToFile, writePprofCaptureToFile } from './download'

function buildMultipartBody(
  boundary: string,
  parts: Array<{
    body: Buffer
    headers: Record<string, string>
  }>,
  options?: {
    close?: boolean
  }
) {
  const chunks: Buffer[] = []

  for (const part of parts) {
    const headerBlock =
      `--${boundary}\r\n` +
      Object.entries({
        ...part.headers,
        'Content-Length': `${part.body.byteLength}`,
      })
        .map(([name, value]) => `${name}: ${value}\r\n`)
        .join('') +
      '\r\n'

    chunks.push(Buffer.from(headerBlock, 'utf8'))
    chunks.push(part.body)
    chunks.push(Buffer.from('\r\n', 'utf8'))
  }

  if (options?.close !== false) {
    chunks.push(Buffer.from(`--${boundary}--\r\n`, 'utf8'))
  }

  return Buffer.concat(chunks)
}

function splitBuffer(buffer: Buffer, chunkSize: number) {
  const chunks: Buffer[] = []

  for (let index = 0; index < buffer.length; index += chunkSize) {
    chunks.push(buffer.subarray(index, index + chunkSize))
  }

  return chunks
}

function createIndexedAccessCountingChunk(content: Buffer) {
  let indexedAccesses = 0
  const bytes = new Uint8Array(content)
  const chunk = new Proxy(bytes, {
    get(target, property, receiver) {
      if (typeof property === 'string' && /^\d+$/.test(property)) {
        indexedAccesses += 1
      }

      if (property === 'byteLength' || property === 'length' || property === 'buffer') {
        return Reflect.get(target, property, target)
      }

      return Reflect.get(target, property, receiver)
    },
  })

  return {
    chunk,
    get indexedAccesses() {
      return indexedAccesses
    },
  }
}

describe('writeMultipartPprofToFile', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pprof-download-'))
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('writes the multipart profile part to disk while ignoring heartbeat parts', async () => {
    const boundary = 'pprof-test-boundary'
    const profile = Buffer.from([1, 2, 3, 4, 5, 6])
    const outputPath = path.join(tempDir, 'nested', 'profile.pprof')
    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'storage-cpu.pprof',
            seconds: 60,
            servingWorkerId: 2,
            type: 'cpu',
            workerCount: 2,
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            at: '2026-04-17T12:00:00.000Z',
            event: 'ping',
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Disposition': 'attachment; filename="storage-cpu.pprof"',
          'Content-Type': 'application/octet-stream',
        },
        body: profile,
      },
    ])

    const response = Readable.from(splitBuffer(body, 7))

    const result = await writeMultipartPprofToFile(
      response,
      `multipart/mixed; boundary="${boundary}"`,
      {
        outputPath,
      }
    )

    await expect(fs.readFile(outputPath)).resolves.toEqual(profile)
    expect(result.outputPath).toBe(outputPath)
    expect(result.startedEvent).toMatchObject({
      applicationId: 'storage',
      event: 'started',
      servingWorkerId: 2,
      type: 'cpu',
    })
  })

  it('rejects when the server streams an error part', async () => {
    const boundary = 'pprof-test-boundary'
    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'storage-heap.pprof',
            seconds: 10,
            servingWorkerId: 2,
            type: 'heap',
            workerId: 7,
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            event: 'error',
            error: {
              code: 'PLT_CTR_FAILED_TO_STOP_PROFILING',
              message: 'stop failed',
              statusCode: 502,
            },
          }),
          'utf8'
        ),
      },
    ])

    const response = Readable.from(splitBuffer(body, 11))

    await expect(
      writeMultipartPprofToFile(response, `multipart/mixed; boundary=${boundary}`, {
        outputPath: path.join(tempDir, 'profile.pprof'),
      })
    ).rejects.toThrow('[PLT_CTR_FAILED_TO_STOP_PROFILING] stop failed')
  })

  it('sanitizes server-provided filenames before writing the profile to dist', async () => {
    const boundary = 'pprof-test-boundary'
    const profile = Buffer.from([7, 8, 9])
    vi.spyOn(process, 'cwd').mockReturnValue(tempDir)

    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'storage-cpu.pprof',
            seconds: 60,
            servingWorkerId: 2,
            type: 'cpu',
            workerCount: 2,
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Disposition': 'attachment; filename="..\\\\..\\\\evil.pprof"',
          'Content-Type': 'application/octet-stream',
        },
        body: profile,
      },
    ])

    const result = await writeMultipartPprofToFile(
      Readable.from(splitBuffer(body, 9)),
      `multipart/mixed; boundary=${boundary}`
    )

    const expectedOutputPath = path.join(tempDir, 'dist', 'evil.pprof')
    expect(result.outputPath).toBe(expectedOutputPath)
    await expect(fs.readFile(expectedOutputPath)).resolves.toEqual(profile)
  })

  it('falls back to quoted filenames when extended filename decoding fails', async () => {
    const boundary = 'pprof-test-boundary'
    const profile = Buffer.from([10, 11, 12])
    vi.spyOn(process, 'cwd').mockReturnValue(tempDir)

    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'started-fallback.pprof',
            seconds: 30,
            servingWorkerId: 2,
            type: 'cpu',
            workerCount: 2,
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Disposition':
            'attachment; filename*=UTF-8\'\'broken%ZZ; filename="quoted.pprof"',
          'Content-Type': 'application/octet-stream',
        },
        body: profile,
      },
    ])

    const result = await writeMultipartPprofToFile(
      Readable.from(splitBuffer(body, 9)),
      `multipart/mixed; boundary=${boundary}`
    )

    const expectedOutputPath = path.join(tempDir, 'dist', 'quoted.pprof')
    expect(result.outputPath).toBe(expectedOutputPath)
    await expect(fs.readFile(expectedOutputPath)).resolves.toEqual(profile)
  })

  it('surfaces UND_ERR_SOCKET disconnects with a clearer capture error', async () => {
    const boundary = 'pprof-test-boundary'
    const body = buildMultipartBody(
      boundary,
      [
        {
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: Buffer.from(
            JSON.stringify({
              applicationId: 'storage',
              event: 'started',
              filename: 'storage-cpu.pprof',
              seconds: 300,
              servingWorkerId: 2,
              type: 'cpu',
            }),
            'utf8'
          ),
        },
        {
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: Buffer.from(
            JSON.stringify({
              at: '2026-04-17T19:04:26.750Z',
              event: 'ping',
            }),
            'utf8'
          ),
        },
      ],
      {
        close: false,
      }
    )

    const cause = Object.assign(new Error('upstream transport lost'), {
      code: 'UND_ERR_SOCKET',
    })
    const disconnectedError = new Error('stream aborted', { cause })
    const response = Readable.from(
      (async function* () {
        yield body
        throw disconnectedError
      })()
    )

    let error: unknown

    try {
      await writeMultipartPprofToFile(response, `multipart/mixed; boundary=${boundary}`, {
        outputPath: path.join(tempDir, 'profile.pprof'),
      })
    } catch (caught) {
      error = caught
    }

    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toContain(
      'Pprof capture stream ended before the profile was delivered for storage (cpu, 300s).'
    )
    expect((error as Error).message).toContain(
      'Last heartbeat arrived at 2026-04-17T19:04:26.750Z.'
    )
    expect((error as Error).message).toContain('Serving worker: 2.')
    expect((error as Error).message).toContain('The connection died mid-capture')
    expect((error as Error).message).toContain('load balancer')
    expect((error as Error).message).toContain('serving worker exited')
    expect((error as Error).cause).toBe(disconnectedError)
  })

  it('surfaces the same diagnostic on clean EOF after capture start', async () => {
    const boundary = 'pprof-test-boundary'
    const body = buildMultipartBody(
      boundary,
      [
        {
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: Buffer.from(
            JSON.stringify({
              applicationId: 'storage',
              event: 'started',
              filename: 'storage-heap.pprof',
              seconds: 120,
              servingWorkerId: 2,
              type: 'heap',
            }),
            'utf8'
          ),
        },
        {
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: Buffer.from(
            JSON.stringify({
              at: '2026-04-17T20:00:00.000Z',
              event: 'ping',
            }),
            'utf8'
          ),
        },
      ],
      {
        close: false,
      }
    )

    let error: unknown

    try {
      await writeMultipartPprofToFile(
        Readable.from(splitBuffer(body, 13)),
        `multipart/mixed; boundary=${boundary}`,
        {
          outputPath: path.join(tempDir, 'profile.pprof'),
        }
      )
    } catch (caught) {
      error = caught
    }

    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toContain(
      'Pprof capture stream ended before the profile was delivered for storage (heap, 120s).'
    )
    expect((error as Error).message).toContain(
      'Last heartbeat arrived at 2026-04-17T20:00:00.000Z.'
    )
    expect((error as Error).message).toContain('Serving worker: 2.')
    expect((error as Error).cause).toBeUndefined()
  })

  it('rethrows terminated stream errors before the capture starts', async () => {
    const boundary = 'pprof-test-boundary'
    const terminatedError = new TypeError('terminated')
    const response = Readable.from(
      (async function* () {
        throw terminatedError
      })()
    )

    await expect(
      writeMultipartPprofToFile(response, `multipart/mixed; boundary=${boundary}`, {
        outputPath: path.join(tempDir, 'profile.pprof'),
      })
    ).rejects.toBe(terminatedError)
  })

  it('surfaces an exact undici terminated error after capture start', async () => {
    const boundary = 'pprof-test-boundary'
    const body = buildMultipartBody(
      boundary,
      [
        {
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
          },
          body: Buffer.from(
            JSON.stringify({
              applicationId: 'storage',
              event: 'started',
              filename: 'storage-cpu.pprof',
              seconds: 45,
              servingWorkerId: 2,
              type: 'cpu',
            }),
            'utf8'
          ),
        },
      ],
      {
        close: false,
      }
    )

    const terminatedError = new TypeError('terminated')
    const response = Readable.from(
      (async function* () {
        yield body
        throw terminatedError
      })()
    )

    await expect(
      writeMultipartPprofToFile(response, `multipart/mixed; boundary=${boundary}`, {
        outputPath: path.join(tempDir, 'profile.pprof'),
      })
    ).rejects.toMatchObject({
      cause: terminatedError,
      message: expect.stringContaining(
        'Pprof capture stream ended before the profile was delivered for storage (cpu, 45s).'
      ),
    })
  })

  it('does not add an error listener per backpressured profile write', async () => {
    const boundary = 'pprof-test-boundary'
    const profile = Buffer.alloc(24, 0x1)
    const outputPath = path.join(tempDir, 'profile.pprof')
    const writeStream = new Writable({
      highWaterMark: 1,
      write(_chunk, _encoding, callback) {
        setImmediate(callback)
      },
    })
    vi.spyOn(nodeFs, 'createWriteStream').mockReturnValue(writeStream as nodeFs.WriteStream)

    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'storage-cpu.pprof',
            seconds: 60,
            servingWorkerId: 2,
            type: 'cpu',
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Disposition': 'attachment; filename="storage-cpu.pprof"',
          'Content-Type': 'application/octet-stream',
        },
        body: profile,
      },
    ])

    await writeMultipartPprofToFile(
      Readable.from(splitBuffer(body, 1)),
      `multipart/mixed; boundary="${boundary}"`,
      {
        outputPath,
      }
    )

    expect(writeStream.listenerCount('error')).toBeLessThan(6)
  })
})

describe('writePprofCaptureToFile', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pprof-download-'))
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('writes raw full heap snapshot streams using the response filename', async () => {
    const snapshot = Buffer.from('{"snapshot":true}')
    vi.spyOn(process, 'cwd').mockReturnValue(tempDir)

    const result = await writePprofCaptureToFile(Readable.from(splitBuffer(snapshot, 5)), {
      contentDisposition: 'attachment; filename="..\\\\storage-worker-7.heapsnapshot"',
      contentType: 'application/octet-stream',
      type: 'heap-snapshot',
    })

    const expectedOutputPath = path.join(tempDir, 'dist', 'storage-worker-7.heapsnapshot')
    expect(result.outputPath).toBe(expectedOutputPath)
    expect(result).not.toHaveProperty('startedEvent')
    await expect(fs.readFile(expectedOutputPath)).resolves.toEqual(snapshot)
  })

  it('uses a heap-snapshot fallback filename for malformed raw capture filenames', async () => {
    const snapshot = Buffer.from('{"snapshot":true}')
    vi.spyOn(process, 'cwd').mockReturnValue(tempDir)

    const result = await writePprofCaptureToFile(Readable.from([snapshot]), {
      contentDisposition: 'attachment; filename=".."',
      contentType: 'application/octet-stream',
      type: 'heap-snapshot',
    })

    const expectedOutputPath = path.join(tempDir, 'dist', 'heap-snapshot.heapsnapshot')
    expect(result.outputPath).toBe(expectedOutputPath)
    await expect(fs.readFile(expectedOutputPath)).resolves.toEqual(snapshot)
  })

  it('rejects empty raw full heap snapshot responses', async () => {
    await expect(
      writePprofCaptureToFile(Readable.from([]), {
        contentDisposition: 'attachment; filename="empty.heapsnapshot"',
        contentType: 'application/octet-stream',
        type: 'heap-snapshot',
      })
    ).rejects.toThrow('Heap snapshot response was empty.')
  })

  it('rejects truncated raw full heap snapshot responses', async () => {
    await expect(
      writePprofCaptureToFile(Readable.from(['{"snapshot":true']), {
        contentDisposition: 'attachment; filename="truncated.heapsnapshot"',
        contentType: 'application/octet-stream',
        type: 'heap-snapshot',
      })
    ).rejects.toThrow('Heap snapshot response is not a complete JSON object.')
  })

  it('does not scan every byte in heap snapshot chunks after finding the opening brace', async () => {
    const writeStream = new Writable({
      objectMode: true,
      write(_chunk, _encoding, callback) {
        callback()
      },
    })
    vi.spyOn(nodeFs, 'createWriteStream').mockReturnValue(writeStream as nodeFs.WriteStream)

    const firstChunk = createIndexedAccessCountingChunk(Buffer.from('{'))
    const middleChunk = createIndexedAccessCountingChunk(Buffer.alloc(10_000, 0x78))
    const lastChunk = createIndexedAccessCountingChunk(Buffer.from('}'))

    await writePprofCaptureToFile(
      Readable.from([firstChunk.chunk, middleChunk.chunk, lastChunk.chunk]),
      {
        contentDisposition: 'attachment; filename="profile.heapsnapshot"',
        contentType: 'application/octet-stream',
        type: 'heap-snapshot',
      },
      {
        outputPath: path.join(tempDir, 'profile.heapsnapshot'),
      }
    )

    expect(middleChunk.indexedAccesses).toBeLessThan(10)
  })

  it('keeps pprof captures on the multipart parser even with malformed response content-type', async () => {
    await expect(
      writePprofCaptureToFile(Readable.from(['profile-data']), {
        contentType: 'application/octet-stream',
        type: 'heap',
      })
    ).rejects.toThrow('Expected a multipart/mixed pprof response with a boundary parameter.')
  })

  it('keeps multipart pprof behavior behind the generic capture writer', async () => {
    const boundary = 'pprof-test-boundary'
    const profile = Buffer.from([1, 2, 3])
    const outputPath = path.join(tempDir, 'profile.pprof')
    const body = buildMultipartBody(boundary, [
      {
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
        },
        body: Buffer.from(
          JSON.stringify({
            applicationId: 'storage',
            event: 'started',
            filename: 'storage-cpu.pprof',
            seconds: 60,
            type: 'cpu',
          }),
          'utf8'
        ),
      },
      {
        headers: {
          'Content-Disposition': 'attachment; filename="storage-cpu.pprof"',
          'Content-Type': 'application/octet-stream',
        },
        body: profile,
      },
    ])

    const result = await writePprofCaptureToFile(
      Readable.from(splitBuffer(body, 7)),
      {
        contentType: `multipart/mixed; boundary=${boundary}`,
        type: 'profile',
      },
      {
        outputPath,
      }
    )

    expect(result.outputPath).toBe(outputPath)
    expect(result.startedEvent).toMatchObject({
      applicationId: 'storage',
      type: 'cpu',
    })
    await expect(fs.readFile(outputPath)).resolves.toEqual(profile)
  })
})
