import { vi } from 'vitest'

const getGlobalMock = vi.hoisted(() => vi.fn())
const getManagementMock = vi.hoisted(() => vi.fn())

vi.mock('@platformatic/globals', () => ({
  getGlobal: getGlobalMock,
  getManagement: getManagementMock,
}))

import { FailedToStartProfiling } from '@platformatic/control'
// @ts-expect-error the deep errors module ships no type declarations
import { WorkerNotFoundError } from '@platformatic/runtime/lib/errors.js'
import {
  asProfilingRuntimeApiClient,
  buildHeapSnapshotFilename,
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
  resolveRuntimeWorkerIdsFromError,
  resolveWattPprofSelection,
} from './runtime'
import type { ProfilingRuntimeApiClient, WattPprofSelection } from './types'

// The shipped platformatic .d.ts files lag the real @fastify/error constructors, so re-type
// them as plain error constructors for fixture building.
type PlatformaticErrorConstructor = new (...args: (string | number)[]) => Error
const workerNotFoundError = WorkerNotFoundError as PlatformaticErrorConstructor
const failedToStartProfilingError =
  FailedToStartProfiling as unknown as PlatformaticErrorConstructor

function createClient(overrides?: {
  close?: ProfilingRuntimeApiClient['close']
  getRuntimeApplications?: ProfilingRuntimeApiClient['getRuntimeApplications']
  startApplicationProfiling?: ProfilingRuntimeApiClient['startApplicationProfiling']
  stopApplicationProfiling?: ProfilingRuntimeApiClient['stopApplicationProfiling']
  takeApplicationHeapSnapshot?: ProfilingRuntimeApiClient['takeApplicationHeapSnapshot']
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
    takeApplicationHeapSnapshot:
      overrides?.takeApplicationHeapSnapshot ?? vi.fn().mockResolvedValue(undefined),
  } satisfies ProfilingRuntimeApiClient
}

describe('pprof runtime helpers', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    getManagementMock.mockReturnValue(undefined)
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

  describe('buildHeapSnapshotFilename', () => {
    it('sanitizes application ids and includes worker ids when present', () => {
      expect(
        buildHeapSnapshotFilename({
          applicationId: 'storage/api v1',
          runtimePid: process.pid,
          targetApplicationId: 'storage/api v1:3',
          workerId: 3,
        })
      ).toBe('storage-api-v1-worker-3.heapsnapshot')
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

      expect(
        getKnownPprofError(
          Object.assign(new Error('snapshot failed'), {
            code: PPROF_CONTROL_ERROR_CODES.failedToTakeHeapSnapshot,
          })
        )
      ).toEqual({
        code: PPROF_CONTROL_ERROR_CODES.failedToTakeHeapSnapshot,
        message: 'snapshot failed',
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

    it('uses live worker ids when the runtime exposes non-dense worker details', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [
            {
              id: 'storage',
              workers: [{ worker: 4 }, { worker: 5 }],
            },
          ],
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
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:5',
            workerId: 5,
          },
        ],
      })
    })

    it('uses the management worker map before falling back to count-based selection', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })
      getManagementMock.mockReturnValue({
        getWorkers: vi.fn().mockResolvedValue({
          'storage:4': {
            application: 'storage',
            worker: '4',
            status: 'started',
            thread: 11,
          },
          'storage:5': {
            application: 'storage',
            worker: '5',
            status: 'started',
            thread: 12,
          },
          'worker-service:2': {
            application: 'worker-service',
            worker: '2',
            status: 'started',
            thread: 13,
          },
        }),
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [{ id: 'storage', workers: 2 }],
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
          {
            applicationId: 'storage',
            runtimePid: process.pid,
            targetApplicationId: 'storage:5',
            workerId: 5,
          },
        ],
      })
      expect(client.getRuntimeApplications).not.toHaveBeenCalled()
    })

    it('ignores non-started workers from the management worker map', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })
      getManagementMock.mockReturnValue({
        getWorkers: vi.fn().mockResolvedValue({
          'storage:4': {
            application: 'storage',
            worker: '4',
            status: 'started',
            thread: 11,
          },
          'storage:5': {
            application: 'storage',
            worker: '5',
            status: 'starting',
            thread: 12,
          },
          'storage:6': {
            application: 'storage',
            worker: '6',
            status: 'stopping',
            thread: 13,
          },
        }),
      })

      const client = createClient()

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
      expect(client.getRuntimeApplications).not.toHaveBeenCalled()
    })

    it('falls back to runtime application discovery when management workers are unavailable', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })
      getManagementMock.mockReturnValue({
        getWorkers: vi.fn().mockRejectedValue(new Error('Operation "getWorkers" is not allowed')),
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [{ id: 'storage', workers: 2 }],
        }),
      })

      await expect(resolveWattPprofSelection(client, undefined)).resolves.toEqual({
        applicationId: 'storage',
        requestedWorkerId: undefined,
        runtimePid: process.pid,
        servingWorkerId: 4,
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
      })
      expect(client.getRuntimeApplications).toHaveBeenCalledWith(process.pid)
    })

    it('falls back to dense worker ids when worker details are only partially recognized', async () => {
      getGlobalMock.mockReturnValue({
        applicationId: 'storage',
        workerId: '4',
      })

      const client = createClient({
        getRuntimeApplications: vi.fn().mockResolvedValue({
          applications: [
            {
              id: 'storage',
              workers: [{ worker: 4 }, { status: 'starting' }],
            },
          ],
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

  describe('resolveRuntimeWorkerIdsFromError', () => {
    it('extracts live worker ids from the installed platformatic error classes', () => {
      // Built from the real error classes so a wattpm upgrade that rewords the message turns
      // this into a failing test instead of a silently dead retry.
      const workerNotFound = new workerNotFoundError(0, 'storage', '4, 5')
      const failedToStart = new failedToStartProfilingError('storage:0', workerNotFound.message)

      expect(failedToStart.message).toBe(
        'Failed to start profiling for service "storage:0": ' +
          'Worker 0 of application storage not found. Available workers are: 4, 5'
      )
      expect(resolveRuntimeWorkerIdsFromError(failedToStart)).toEqual([4, 5])
    })

    it('keeps the full id list when text follows it', () => {
      expect(
        resolveRuntimeWorkerIdsFromError(
          new Error(
            'Worker 0 of application storage not found. Available applications are: 4, 5\n2 workers required'
          )
        )
      ).toEqual([4, 5])
    })

    it('returns undefined for errors without a worker-not-found message', () => {
      expect(
        resolveRuntimeWorkerIdsFromError(
          new Error('Profiling is already started for application "storage" (all workers).')
        )
      ).toBeUndefined()
      expect(
        resolveRuntimeWorkerIdsFromError(
          new Error('Application storage not found. Available applications are: 4, 5')
        )
      ).toBeUndefined()
      expect(resolveRuntimeWorkerIdsFromError(undefined)).toBeUndefined()
    })
  })
})
