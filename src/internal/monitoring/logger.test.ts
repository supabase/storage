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
})
