import { vi } from 'vitest'

const getGlobalMock = vi.hoisted(() => vi.fn())

vi.mock('@platformatic/globals', () => ({
  getGlobal: getGlobalMock,
}))

import {
  asProfilingRuntimeApiClient,
  buildPprofFilename,
  buildPprofResponseHeaders,
  buildPprofSessionKey,
  buildPprofStartOptions,
  createControlStyleError,
  getKnownPprofError,
  isAbortError,
  normalizeNodeModulesSourceMaps,
  PPROF_CONTROL_ERROR_CODES,
  resolvePprofFilenameTarget,
  resolveWattPprofSelection,
} from './runtime'
import type { ProfilingRuntimeApiClient, WattPprofSelection } from './types'

function createClient(overrides?: {
  close?: ProfilingRuntimeApiClient['close']
  getRuntimeApplications?: ProfilingRuntimeApiClient['getRuntimeApplications']
  startApplicationProfiling?: ProfilingRuntimeApiClient['startApplicationProfiling']
  stopApplicationProfiling?: ProfilingRuntimeApiClient['stopApplicationProfiling']
}) {
  return {
    close: overrides?.close ?? vi.fn().mockResolvedValue(undefined),
    getRuntimeApplications:
      overrides?.getRuntimeApplications ??
      vi.fn().mockResolvedValue({
        applications: [{ id: 'storage', workers: 2 }],
      }),
    startApplicationProfiling:
      overrides?.startApplicationProfiling ?? vi.fn().mockResolvedValue(undefined),
    stopApplicationProfiling:
      overrides?.stopApplicationProfiling ?? vi.fn().mockResolvedValue(new ArrayBuffer(0)),
  } satisfies ProfilingRuntimeApiClient
}

describe('pprof runtime helpers', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('normalizeNodeModulesSourceMaps', () => {
    it('trims, de-duplicates, and drops empty values', () => {
      expect(normalizeNodeModulesSourceMaps(undefined)).toBeUndefined()
      expect(normalizeNodeModulesSourceMaps(' next, ,@next/next-server,next ')).toEqual([
        'next',
        '@next/next-server',
      ])
      expect(
        normalizeNodeModulesSourceMaps([' next ', '@next/next-server', 'next,@next/next-server'])
      ).toEqual(['next', '@next/next-server'])
    })
  })

  describe('buildPprofFilename', () => {
    it('sanitizes application ids and includes worker ids when present', () => {
      expect(
        buildPprofFilename(
          {
            applicationId: 'storage/api v1',
            runtimePid: process.pid,
            targetApplicationId: 'storage/api v1:3',
            workerId: 3,
          },
          'cpu'
        )
      ).toBe('storage-api-v1-worker-3-cpu.pprof')
    })
  })

  describe('resolvePprofFilenameTarget', () => {
    it('drops the worker id for whole-app captures', () => {
      const selection: WattPprofSelection = {
        applicationId: 'storage',
        runtimePid: process.pid,
        scopeKey: 'all',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:0',
            workerId: 0,
          },
        ],
      }

      expect(resolvePprofFilenameTarget(selection)).toMatchObject({
        applicationId: 'storage',
        workerId: undefined,
      })
    })
  })

  describe('buildPprofResponseHeaders', () => {
    it('emits worker-count headers for whole-app captures', () => {
      const selection: WattPprofSelection = {
        applicationId: 'storage',
        runtimePid: process.pid,
        scopeKey: 'all',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:0',
            workerId: 0,
          },
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:1',
            workerId: 1,
          },
        ],
      }

      expect(buildPprofResponseHeaders(selection, 'multipart/mixed; boundary=test')).toEqual({
        'cache-control': 'no-store',
        'content-type': 'multipart/mixed; boundary=test',
        'x-platformatic-application-id': 'storage',
        'x-platformatic-worker-count': '2',
      })
    })
  })

  describe('isAbortError', () => {
    it('recognizes DOM and Node abort errors', () => {
      expect(isAbortError(new DOMException('Aborted', 'AbortError'))).toBe(true)
      expect(isAbortError(Object.assign(new Error('aborted'), { code: 'ABORT_ERR' }))).toBe(true)
      expect(isAbortError(new Error('other'))).toBe(false)
    })
  })

  describe('createControlStyleError', () => {
    it('attaches the control error code to the error object', () => {
      const error = createControlStyleError(
        PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
        'busy'
      )

      expect(error).toBeInstanceOf(Error)
      expect(error).toMatchObject({
        code: PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
        message: 'busy',
      })
    })
  })

  describe('getKnownPprofError', () => {
    it('maps known control errors to status codes', () => {
      expect(
        getKnownPprofError(
          Object.assign(new Error('busy'), {
            code: PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
          })
        )
      ).toEqual({
        code: PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
        message: 'busy',
        statusCode: 409,
      })

      expect(
        getKnownPprofError(
          Object.assign(new Error('app missing'), {
            code: PPROF_CONTROL_ERROR_CODES.applicationNotFound,
          })
        )
      ).toEqual({
        code: PPROF_CONTROL_ERROR_CODES.applicationNotFound,
        message: 'app missing',
        statusCode: 503,
      })

      expect(
        getKnownPprofError(
          Object.assign(new Error('control failed'), {
            code: PPROF_CONTROL_ERROR_CODES.failedToStart,
            cause: Object.assign(
              new Error("Cannot find package '@platformatic/wattpm-pprof-capture'"),
              {
                code: 'ERR_MODULE_NOT_FOUND',
              }
            ),
          })
        )
      ).toEqual({
        code: PPROF_CONTROL_ERROR_CODES.failedToStart,
        message: 'control failed',
        statusCode: 501,
      })

      expect(
        getKnownPprofError(
          Object.assign(new Error('missing @platformatic/wattpm-pprof-capture dependency'), {
            code: PPROF_CONTROL_ERROR_CODES.failedToStart,
          })
        )
      ).toEqual({
        code: PPROF_CONTROL_ERROR_CODES.failedToStart,
        message: 'missing @platformatic/wattpm-pprof-capture dependency',
        statusCode: 502,
      })

      expect(getKnownPprofError(new Error('other'))).toBeUndefined()
    })
  })

  describe('asProfilingRuntimeApiClient', () => {
    it('accepts clients with the profiling control shape', () => {
      const client = createClient()

      expect(asProfilingRuntimeApiClient(client)).toBe(client)
    })

    it('rejects clients that are missing profiling control methods', () => {
      expect(() =>
        asProfilingRuntimeApiClient({
          close: async () => {},
          getRuntimeApplications: async () => ({ applications: [] }),
        })
      ).toThrow('RuntimeApiClient does not expose the profiling control methods.')
    })
  })

  describe('buildPprofSessionKey', () => {
    it('includes application, type, and scope', () => {
      expect(
        buildPprofSessionKey(
          {
            applicationId: 'storage',
            requestedWorkerId: 7,
            runtimePid: process.pid,
            scopeKey: 'worker:7',
            targets: [],
          },
          'heap'
        )
      ).toBe('storage:heap:worker:7')
    })
  })

  describe('buildPprofStartOptions', () => {
    it('adds cpu interval and forces sourceMaps when nodeModulesSourceMaps are present', () => {
      expect(
        buildPprofStartOptions({
          type: 'cpu',
          sourceMaps: false,
          nodeModulesSourceMaps: ['next'],
        })
      ).toEqual({
        type: 'cpu',
        intervalMicros: 1000,
        nodeModulesSourceMaps: ['next'],
        sourceMaps: true,
      })

      expect(
        buildPprofStartOptions({
          type: 'heap',
          sourceMaps: true,
        })
      ).toEqual({
        type: 'heap',
        sourceMaps: true,
      })
    })
  })

  describe('resolveWattPprofSelection', () => {
    it('returns null outside Watt', async () => {
      getGlobalMock.mockReturnValue(undefined)

      await expect(resolveWattPprofSelection(createClient(), undefined)).resolves.toBeNull()
    })

    it('uses the requested worker id directly without enumerating workers', async () => {
      const client = createClient()
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: 2,
      })

      await expect(resolveWattPprofSelection(client, 7)).resolves.toEqual({
        applicationId: 'storage',
        requestedWorkerId: 7,
        runtimePid: process.pid,
        servingWorkerId: 2,
        scopeKey: 'worker:7',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:7',
            workerId: 7,
          },
        ],
      })
      expect(client.getRuntimeApplications).not.toHaveBeenCalled()
    })

    it('enumerates all workers when the application reports more than one worker', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '1',
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [{ id: 'storage', config: { workers: { minimum: 3 } } }],
        }),
      })

      await expect(resolveWattPprofSelection(client, undefined)).resolves.toEqual({
        applicationId: 'storage',
        runtimePid: process.pid,
        servingWorkerId: 1,
        scopeKey: 'all',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:0',
            workerId: 0,
          },
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:1',
            workerId: 1,
          },
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:2',
            workerId: 2,
          },
        ],
      })
    })

    it('falls back to the current worker when only one worker is reported', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [{ id: 'storage', workers: 1 }],
        }),
      })

      await expect(resolveWattPprofSelection(client, undefined)).resolves.toEqual({
        applicationId: 'storage',
        runtimePid: process.pid,
        servingWorkerId: 4,
        scopeKey: 'all',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:4',
            workerId: 4,
          },
        ],
      })
    })

    it('uses the bare application id when a single-worker app has no worker id in context', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [{ id: 'storage', workers: 1 }],
        }),
      })

      await expect(resolveWattPprofSelection(client, undefined)).resolves.toEqual({
        applicationId: 'storage',
        runtimePid: process.pid,
        servingWorkerId: undefined,
        scopeKey: 'all',
        targets: [
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage',
            workerId: undefined,
          },
        ],
      })
    })
  })
})
