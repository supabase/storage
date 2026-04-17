import fastify, { type FastifyInstance, FastifyReply } from 'fastify'
import {
  Function as PprofFunction,
  Line as PprofLine,
  Location as PprofLocation,
  Mapping as PprofMapping,
  Sample as PprofSample,
  ValueType as PprofValueType,
  Profile,
  StringTable,
} from 'pprof-format'
import { vi } from 'vitest'
import { signals } from '../../plugins/signals'

const runtimeApiClient = vi.hoisted(() => ({
  getRuntimeApplications: vi.fn(),
  startApplicationProfiling: vi.fn(),
  stopApplicationProfiling: vi.fn(),
  close: vi.fn(),
}))
const waitForMultipartPprofWindowMock = vi.hoisted(() => vi.fn())

vi.mock('@platformatic/control', () => ({
  RuntimeApiClient: class MockRuntimeApiClient {
    getRuntimeApplications = runtimeApiClient.getRuntimeApplications
    startApplicationProfiling = runtimeApiClient.startApplicationProfiling
    stopApplicationProfiling = runtimeApiClient.stopApplicationProfiling
    close = runtimeApiClient.close
  },
}))

vi.mock('@platformatic/globals', () => ({
  getGlobal: vi.fn(),
}))

vi.mock('../../plugins/apikey', () => ({
  async default() {},
}))

vi.mock('@internal/monitoring/pprof/multipart', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@internal/monitoring/pprof/multipart')>()

  return {
    ...actual,
    waitForMultipartPprofWindow: waitForMultipartPprofWindowMock,
  }
})

import { getGlobal } from '@platformatic/globals'
import routes from './pprof'

function parseMultipartParts(rawPayload: Buffer, contentType: string | string[] | undefined) {
  const contentTypeValue = Array.isArray(contentType) ? contentType[0] : contentType
  const boundaryMatch = contentTypeValue?.match(/boundary="?([^";]+)"?/i)
  const boundary = boundaryMatch?.[1]

  if (!boundary) {
    throw new Error(`Missing multipart boundary in content type: ${contentTypeValue}`)
  }

  const boundaryLine = `--${boundary}`
  let buffer = rawPayload
  const parts: Array<{
    body: Buffer
    headers: Record<string, string>
  }> = []

  while (buffer.length > 0) {
    const boundaryEnd = buffer.indexOf('\r\n')
    if (boundaryEnd < 0) {
      break
    }

    const line = buffer.subarray(0, boundaryEnd).toString('latin1')
    buffer = buffer.subarray(boundaryEnd + 2)

    if (line === `${boundaryLine}--`) {
      break
    }

    if (line !== boundaryLine) {
      throw new Error(`Unexpected multipart boundary line: ${line}`)
    }

    const headersEnd = buffer.indexOf('\r\n\r\n')
    if (headersEnd < 0) {
      throw new Error('Missing multipart headers terminator')
    }

    const headers = buffer
      .subarray(0, headersEnd)
      .toString('latin1')
      .split('\r\n')
      .reduce<Record<string, string>>((acc, line) => {
        const separator = line.indexOf(':')
        if (separator >= 0) {
          acc[line.slice(0, separator).trim().toLowerCase()] = line.slice(separator + 1).trim()
        }

        return acc
      }, {})

    const contentLength = Number.parseInt(headers['content-length'] || '', 10)
    if (!Number.isSafeInteger(contentLength) || contentLength < 0) {
      throw new Error(`Invalid multipart content length: ${headers['content-length']}`)
    }

    buffer = buffer.subarray(headersEnd + 4)
    const body = buffer.subarray(0, contentLength)
    buffer = buffer.subarray(contentLength)

    if (buffer.subarray(0, 2).toString('latin1') !== '\r\n') {
      throw new Error('Missing multipart body terminator')
    }

    buffer = buffer.subarray(2)
    parts.push({ headers, body: Buffer.from(body) })
  }

  return parts
}

function buildProfile(functionName: string, sampleValue: number) {
  const stringTable = new StringTable()
  const sampleType = new PprofValueType({
    type: stringTable.dedup('samples'),
    unit: stringTable.dedup('count'),
  })

  const profile = new Profile({
    stringTable,
    sampleType: [sampleType],
    periodType: sampleType,
    period: 1,
    timeNanos: 1n,
    durationNanos: 1_000_000_000n,
    mapping: [
      new PprofMapping({
        id: 1,
        hasFunctions: true,
        hasFilenames: true,
        hasLineNumbers: true,
      }),
    ],
    function: [
      new PprofFunction({
        id: 1,
        name: stringTable.dedup(functionName),
        systemName: stringTable.dedup(functionName),
        filename: stringTable.dedup(`${functionName}.ts`),
        startLine: 1,
      }),
    ],
    location: [
      new PprofLocation({
        id: 1,
        mappingId: 1,
        line: [new PprofLine({ functionId: 1, line: 1 })],
      }),
    ],
    sample: [
      new PprofSample({
        locationId: [1],
        value: [sampleValue],
      }),
    ],
    defaultSampleType: stringTable.dedup('samples'),
  })

  return profile.encode()
}

async function buildApp(options?: { onPreHandlerReply?: (reply: FastifyReply) => void }) {
  const app = fastify()
  app.register(signals)
  if (options?.onPreHandlerReply) {
    app.addHook('preHandler', async (_request, reply) => {
      options.onPreHandlerReply?.(reply)
    })
  }
  app.register(routes)
  await app.ready()
  return app
}

type RegisteredRouteHandler = (request: Record<string, unknown>, reply: FastifyReply) => unknown

async function buildRouteHarness() {
  const hooks = new Map<string, () => Promise<void> | void>()
  const handlers = new Map<string, RegisteredRouteHandler>()
  const fastify = {
    addHook: vi.fn((name: string, hook: () => Promise<void> | void) => {
      hooks.set(name, hook)
    }),
    get: vi.fn((path: string, _options: unknown, handler: RegisteredRouteHandler) => {
      handlers.set(path, handler)
    }),
    register: vi.fn(),
  } as unknown as FastifyInstance

  await routes(fastify)

  return {
    getHandler(path: string) {
      const handler = handlers.get(path)

      if (!handler) {
        throw new Error(`Missing registered route handler for ${path}`)
      }

      return handler
    },
    onClose: hooks.get('onClose') ?? (async () => {}),
  }
}

async function clearPprofRouteState() {
  const harness = await buildRouteHarness()
  await harness.onClose()
}

function createDeferredPromise() {
  let resolve!: () => void
  let reject!: (error?: unknown) => void
  const promise = new Promise<void>((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, reject, resolve }
}

function createReplyDouble() {
  const raw = {
    destroyed: false,
    end: vi.fn(),
    socket: {
      setKeepAlive: vi.fn(),
    },
    writableEnded: false,
    write: vi.fn(() => true),
    writeHead: vi.fn(),
  }

  const reply = {
    hijack: vi.fn(),
    raw,
    send: vi.fn(),
    status: vi.fn(),
  }

  reply.status.mockReturnValue(reply)
  reply.send.mockReturnValue(reply)

  return reply as unknown as FastifyReply
}

function emitMultipartPings(
  writer: { writeJsonPart: (payload: unknown) => boolean },
  count: number
) {
  for (let index = 0; index < count; index += 1) {
    writer.writeJsonPart({
      at: `2026-04-17T12:00:0${index}.000Z`,
      event: 'ping',
    })
  }
}

function installMultipartWindowMock(options?: {
  beforeResolve?: () => Promise<void> | void
  pingCount?: number
}) {
  waitForMultipartPprofWindowMock.mockImplementation(
    async (
      reply: FastifyReply,
      writer: { writeJsonPart: (payload: unknown) => boolean },
      seconds: number
    ) => {
      if (typeof reply.raw.socket?.setKeepAlive === 'function') {
        reply.raw.socket.setKeepAlive(true, 5000)
      }

      emitMultipartPings(writer, options?.pingCount ?? Math.floor(seconds / 5))
      await options?.beforeResolve?.()
    }
  )
}

describe('admin pprof routes', () => {
  beforeEach(async () => {
    await clearPprofRouteState()
    vi.clearAllMocks()

    vi.mocked(getGlobal).mockReturnValue({
      applicationId: 'storage',
      workerId: 2,
    } as never)

    runtimeApiClient.getRuntimeApplications.mockResolvedValue({
      applications: [{ id: 'storage', workers: 2 }],
    })
    runtimeApiClient.startApplicationProfiling.mockResolvedValue(undefined)
    runtimeApiClient.stopApplicationProfiling.mockResolvedValue(
      buildProfile('default-worker', 3).buffer
    )
    runtimeApiClient.close.mockResolvedValue(undefined)
    installMultipartWindowMock()
  })

  it('captures a cpu profile for the requested worker via Watt control', async () => {
    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=1&workerId=7&sourceMaps=true',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('multipart/mixed; boundary=')
      expect(response.headers['x-platformatic-application-id']).toBe('storage')
      expect(response.headers['x-platformatic-worker-id']).toBe('7')
      expect(response.headers['content-disposition']).toBeUndefined()

      const parts = parseMultipartParts(response.rawPayload, response.headers['content-type'])
      expect(parts).toHaveLength(2)
      expect(JSON.parse(parts[0].body.toString('utf8'))).toMatchObject({
        applicationId: 'storage',
        event: 'started',
        filename: 'storage-worker-7-cpu.pprof',
        seconds: 1,
        servingWorkerId: 2,
        type: 'cpu',
        workerId: 7,
      })

      const profile = Profile.decode(parts[1].body)
      expect(profile.sample.map((sample) => sample.value[0])).toEqual([3])

      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:7',
        {
          intervalMicros: 1000,
          type: 'cpu',
          sourceMaps: true,
        }
      )
      expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:7',
        {
          type: 'cpu',
        }
      )
      expect(runtimeApiClient.close).toHaveBeenCalledTimes(1)
    } finally {
      await app.close()
    }
  })

  it('passes selected node_modules source maps through to the Watt profiler', async () => {
    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=1&workerId=7&nodeModulesSourceMaps=next,%40next%2Fnext-server,next',
      })

      expect(response.statusCode).toBe(200)
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:7',
        {
          intervalMicros: 1000,
          type: 'cpu',
          sourceMaps: true,
          nodeModulesSourceMaps: ['next', '@next/next-server'],
        }
      )
    } finally {
      await app.close()
    }
  })

  it('accepts repeated nodeModulesSourceMaps query params', async () => {
    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=1&workerId=7&nodeModulesSourceMaps=next&nodeModulesSourceMaps=%40next%2Fnext-server&nodeModulesSourceMaps=next',
      })

      expect(response.statusCode).toBe(200)
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:7',
        {
          intervalMicros: 1000,
          type: 'cpu',
          sourceMaps: true,
          nodeModulesSourceMaps: ['next', '@next/next-server'],
        }
      )
    } finally {
      await app.close()
    }
  })

  it('enables socket keepalive for long cpu captures', async () => {
    let setKeepAliveSpy: ReturnType<typeof vi.fn> | undefined

    const app = await buildApp({
      onPreHandlerReply: (reply) => {
        setKeepAliveSpy = vi.fn()
        if (reply.raw.socket) {
          reply.raw.socket.setKeepAlive = setKeepAliveSpy as typeof reply.raw.socket.setKeepAlive
        }
      },
    })

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=12&workerId=7&sourceMaps=true&nodeModulesSourceMaps=next',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('multipart/mixed; boundary=')
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:7',
        {
          intervalMicros: 1000,
          type: 'cpu',
          sourceMaps: true,
          nodeModulesSourceMaps: ['next'],
        }
      )
      expect(setKeepAliveSpy).toBeDefined()
      expect(setKeepAliveSpy).toHaveBeenCalledTimes(1)
      expect(setKeepAliveSpy).toHaveBeenCalledWith(true, 5000)
    } finally {
      await app.close()
    }
  })

  it('streams multipart pprof parts by default', async () => {
    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=12&workerId=7&sourceMaps=true',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('multipart/mixed; boundary=')
      expect(response.headers['x-platformatic-worker-id']).toBe('7')
      expect(response.headers['content-disposition']).toBeUndefined()

      const parts = parseMultipartParts(response.rawPayload, response.headers['content-type'])
      expect(parts).toHaveLength(4)

      const startedPart = JSON.parse(parts[0].body.toString('utf8'))
      expect(startedPart).toMatchObject({
        applicationId: 'storage',
        event: 'started',
        filename: 'storage-worker-7-cpu.pprof',
        seconds: 12,
        servingWorkerId: 2,
        type: 'cpu',
        workerId: 7,
      })

      expect(JSON.parse(parts[1].body.toString('utf8'))).toMatchObject({ event: 'ping' })
      expect(JSON.parse(parts[2].body.toString('utf8'))).toMatchObject({ event: 'ping' })
      expect(parts[3].headers['content-type']).toBe('application/octet-stream')
      expect(parts[3].headers['content-disposition']).toContain('storage-worker-7-cpu.pprof')

      const profile = Profile.decode(parts[3].body)
      expect(profile.sample.map((sample) => sample.value[0])).toEqual([3])
    } finally {
      await app.close()
    }
  })

  it('rejects overlapping one-shot requests for the same worker and type', async () => {
    const firstWindow = createDeferredPromise()
    let firstCall = true
    installMultipartWindowMock({
      beforeResolve: async () => {
        if (!firstCall) {
          return
        }

        firstCall = false
        await firstWindow.promise
      },
      pingCount: 2,
    })

    const app = await buildApp()

    try {
      const firstWindowStarted = createDeferredPromise()
      waitForMultipartPprofWindowMock.mockImplementationOnce(
        async (reply: FastifyReply, writer: { writeJsonPart: (payload: unknown) => boolean }) => {
          if (typeof reply.raw.socket?.setKeepAlive === 'function') {
            reply.raw.socket.setKeepAlive(true, 5000)
          }

          emitMultipartPings(writer, 2)
          firstWindowStarted.resolve()
          await firstWindow.promise
        }
      )

      const firstResponsePromise = app.inject({
        method: 'GET',
        url: '/profile?seconds=12&workerId=7&sourceMaps=true',
      })

      await firstWindowStarted.promise

      const secondResponse = await app.inject({
        method: 'GET',
        url: '/profile?seconds=1&workerId=7&sourceMaps=true',
      })

      expect(secondResponse.statusCode).toBe(409)
      expect(secondResponse.json()).toEqual({
        message: 'Profiling is already started for service "storage:7".',
      })

      firstWindow.resolve()
      const firstResponse = await firstResponsePromise
      expect(firstResponse.statusCode).toBe(200)
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledTimes(1)
    } finally {
      firstWindow.resolve()
      await app.close()
    }
  })

  it('captures and merges heap profiles across all app workers when workerId is omitted', async () => {
    runtimeApiClient.stopApplicationProfiling
      .mockResolvedValueOnce(buildProfile('worker-zero', 11).buffer)
      .mockResolvedValueOnce(buildProfile('worker-one', 22).buffer)

    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/heap?seconds=1',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('multipart/mixed; boundary=')
      expect(response.headers['x-platformatic-worker-count']).toBe('2')
      expect(response.headers['x-platformatic-worker-id']).toBeUndefined()

      expect(runtimeApiClient.getRuntimeApplications).toHaveBeenCalledWith(process.pid)
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenNthCalledWith(
        1,
        process.pid,
        'storage:0',
        {
          type: 'heap',
        }
      )
      expect(runtimeApiClient.startApplicationProfiling).toHaveBeenNthCalledWith(
        2,
        process.pid,
        'storage:1',
        {
          type: 'heap',
        }
      )
      expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenNthCalledWith(
        1,
        process.pid,
        'storage:0',
        {
          type: 'heap',
        }
      )
      expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenNthCalledWith(
        2,
        process.pid,
        'storage:1',
        {
          type: 'heap',
        }
      )

      const parts = parseMultipartParts(response.rawPayload, response.headers['content-type'])
      expect(parts).toHaveLength(2)
      expect(JSON.parse(parts[0].body.toString('utf8'))).toMatchObject({
        applicationId: 'storage',
        event: 'started',
        filename: 'storage-heap.pprof',
        seconds: 1,
        servingWorkerId: 2,
        type: 'heap',
        workerCount: 2,
      })

      const mergedProfile = Profile.decode(parts[1].body)
      expect(
        mergedProfile.sample
          .map((sample) => sample.value[0])
          .sort((left, right) => Number(left) - Number(right))
      ).toEqual([11, 22])
    } finally {
      await app.close()
    }
  })

  it('rolls back already-started workers when a whole-app start partially fails', async () => {
    runtimeApiClient.startApplicationProfiling
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(
        Object.assign(new Error('start failed'), {
          code: 'PLT_CTR_FAILED_TO_START_PROFILING',
        })
      )

    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/heap?seconds=1',
      })

      expect(response.statusCode).toBe(502)
      expect(response.json()).toEqual({
        message: 'start failed',
      })
      expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledTimes(1)
      expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledWith(
        process.pid,
        'storage:0',
        {
          type: 'heap',
        }
      )
    } finally {
      await app.close()
    }
  })

  it('emits multipart error parts when stop fails after the response has started', async () => {
    runtimeApiClient.stopApplicationProfiling.mockRejectedValue(
      Object.assign(new Error('stop failed'), {
        code: 'PLT_CTR_FAILED_TO_STOP_PROFILING',
      })
    )

    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/heap?seconds=1',
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('multipart/mixed; boundary=')

      const parts = parseMultipartParts(response.rawPayload, response.headers['content-type'])
      expect(parts).toHaveLength(2)
      expect(JSON.parse(parts[0].body.toString('utf8'))).toMatchObject({
        applicationId: 'storage',
        event: 'started',
        servingWorkerId: 2,
        type: 'heap',
        workerCount: 2,
      })
      expect(JSON.parse(parts[1].body.toString('utf8'))).toEqual({
        event: 'error',
        error: {
          code: 'PLT_CTR_FAILED_TO_STOP_PROFILING',
          message: 'stop failed',
          statusCode: 502,
        },
      })
    } finally {
      await app.close()
    }
  })

  it('closes the multipart writer on an aborted in-flight capture', async () => {
    const harness = await buildRouteHarness()
    const reply = createReplyDouble()
    const controller = new AbortController()
    const profileHandler = harness.getHandler('/profile')

    waitForMultipartPprofWindowMock.mockImplementationOnce(
      async (_reply, _writer, _seconds, signal) => {
        expect(signal).toBe(controller.signal)
        controller.abort()
        throw new DOMException('Aborted', 'AbortError')
      }
    )

    await expect(
      profileHandler(
        {
          query: {
            seconds: 30,
            workerId: 7,
          },
          signals: {
            disconnect: controller,
          },
        },
        reply
      )
    ).resolves.toBe(reply)

    expect(runtimeApiClient.startApplicationProfiling).toHaveBeenCalledWith(
      process.pid,
      'storage:7',
      {
        intervalMicros: 1000,
        type: 'cpu',
      }
    )
    expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledWith(
      process.pid,
      'storage:7',
      {
        type: 'cpu',
      }
    )
    expect(runtimeApiClient.close).toHaveBeenCalledTimes(1)
    expect(reply.status).not.toHaveBeenCalled()
    expect(reply.send).not.toHaveBeenCalled()
    expect(reply.raw.end).toHaveBeenCalledTimes(1)
    expect(reply.raw.end).toHaveBeenCalledWith(expect.stringMatching(/^--pprof-.*--\r\n$/))

    await harness.onClose()
  })

  it('drains active sessions through the registered onClose hook', async () => {
    const harness = await buildRouteHarness()
    const reply = createReplyDouble()
    const keepOpen = createDeferredPromise()
    const windowStarted = createDeferredPromise()
    const profileHandler = harness.getHandler('/profile')

    waitForMultipartPprofWindowMock.mockImplementationOnce(async () => {
      windowStarted.resolve()
      await keepOpen.promise
    })

    const handlerPromise = profileHandler(
      {
        query: {
          seconds: 30,
          workerId: 7,
        },
        signals: {
          disconnect: new AbortController(),
        },
      },
      reply
    )

    await windowStarted.promise
    await harness.onClose()

    expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledTimes(1)
    expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledWith(
      process.pid,
      'storage:7',
      {
        type: 'cpu',
      }
    )

    keepOpen.resolve()
    await expect(handlerPromise).resolves.toBe(reply)
    expect(reply.raw.end).toHaveBeenCalledTimes(1)
    expect(runtimeApiClient.stopApplicationProfiling).toHaveBeenCalledTimes(1)
    expect(runtimeApiClient.close).toHaveBeenCalledTimes(2)
  })

  it('returns 501 when the admin app is not running under Watt', async () => {
    vi.mocked(getGlobal).mockReturnValue(undefined)

    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/profile?seconds=1',
      })

      expect(response.statusCode).toBe(501)
      expect(response.json()).toEqual({
        message: 'pprof capture is only available when running under Platformatic Watt.',
      })
      expect(runtimeApiClient.startApplicationProfiling).not.toHaveBeenCalled()
      expect(runtimeApiClient.stopApplicationProfiling).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('maps control profiling conflicts to a 409 response', async () => {
    runtimeApiClient.startApplicationProfiling.mockRejectedValue(
      Object.assign(new Error('Profiling is already started for service "storage:2".'), {
        code: 'PLT_CTR_PROFILING_ALREADY_STARTED',
      })
    )

    const app = await buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/heap',
      })

      expect(response.statusCode).toBe(409)
      expect(response.json()).toEqual({
        message: 'Profiling is already started for service "storage:2".',
      })
      expect(runtimeApiClient.close).toHaveBeenCalledTimes(1)
    } finally {
      await app.close()
    }
  })
})
