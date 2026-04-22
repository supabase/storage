import { vi } from 'vitest'

const mockedLoggerModules = ['pino', '../../config'] as const

describe('logger serializers', () => {
  afterEach(() => {
    for (const moduleId of mockedLoggerModules) {
      vi.doUnmock(moduleId)
    }

    vi.restoreAllMocks()
    vi.resetModules()
  })

  test('res serializer tolerates synthetic replies without getHeaders', async () => {
    let resSerializer:
      | ((reply: { statusCode: number; getHeaders?: () => Record<string, unknown> }) => unknown)
      | undefined

    const loggerStub = {
      child: vi.fn(),
      debug: vi.fn(),
      error: vi.fn(),
      fatal: vi.fn(),
      flush: vi.fn(),
      info: vi.fn(),
      level: 'info',
      silent: vi.fn(),
      trace: vi.fn(),
      warn: vi.fn(),
    }
    loggerStub.child.mockReturnValue(loggerStub)

    const pinoMock = Object.assign(
      vi.fn((options: { serializers: { res: typeof resSerializer } }) => {
        resSerializer = options.serializers.res
        return loggerStub
      }),
      {
        stdTimeFunctions: {
          isoTime: vi.fn(),
        },
      }
    )

    vi.doMock('pino', () => ({
      default: pinoMock,
      Logger: class Logger {},
    }))
    vi.doMock('../../config', () => ({
      getConfig: vi.fn(() => ({
        logLevel: 'info',
        logflareApiKey: undefined,
        logflareBatchSize: 1,
        logflareEnabled: false,
        logflareSourceToken: undefined,
        region: 'local',
      })),
    }))

    await import('./logger')

    expect(resSerializer).toBeDefined()
    expect(() => resSerializer?.({ statusCode: 503 })).not.toThrow()
    expect(resSerializer?.({ statusCode: 503 })).toEqual({
      headers: {},
      statusCode: 503,
    })
  })

  test('buildTransport wires logflare hooks when logflare is enabled', async () => {
    const loggerStub = {
      child: vi.fn(),
      debug: vi.fn(),
      error: vi.fn(),
      fatal: vi.fn(),
      flush: vi.fn(),
      info: vi.fn(),
      level: 'info',
      silent: vi.fn(),
      trace: vi.fn(),
      warn: vi.fn(),
    }
    loggerStub.child.mockReturnValue(loggerStub)

    const pinoMock = Object.assign(
      vi.fn(() => loggerStub),
      {
        stdTimeFunctions: {
          isoTime: vi.fn(),
        },
      }
    )

    vi.doMock('pino', () => ({
      default: pinoMock,
      Logger: class Logger {},
    }))
    vi.doMock('../../config', () => ({
      getConfig: vi.fn(() => ({
        logLevel: 'debug',
        logflareApiKey: 'api-key',
        logflareBatchSize: 25,
        logflareEnabled: true,
        logflareSourceToken: 'source-token',
        region: 'local',
      })),
    }))

    const { buildTransport } = await import('./logger')
    interface LogflareTransportOptions {
      apiKey: string
      sourceToken: string
      batchSize: number
      onPreparePayload: { module: string; method: string }
      onError: { module: string; method: string }
    }
    const transport = buildTransport() as {
      targets: Array<{
        level: string
        target: string
        options: LogflareTransportOptions
      }>
    }

    const logflareTarget = transport.targets.find((target) => target.target === 'pino-logflare')

    expect(logflareTarget).toMatchObject({
      level: 'debug',
      target: 'pino-logflare',
      options: {
        apiKey: 'api-key',
        sourceToken: 'source-token',
        batchSize: 25,
        onPreparePayload: {
          method: 'onPreparePayload',
        },
        onError: {
          method: 'onError',
        },
      },
    })
    expect(logflareTarget?.options.onPreparePayload.module).toMatch(/logflare$/)
    expect(logflareTarget?.options.onPreparePayload.module).toBe(
      logflareTarget?.options.onError.module
    )
  })
})
