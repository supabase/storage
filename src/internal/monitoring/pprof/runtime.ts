import { getGlobal } from '@platformatic/globals'
import type {
  PprofCaptureType,
  PprofKnownError,
  ProfilingRuntimeApiClient,
  RuntimeApplicationWorkersShape,
  WattPprofSelection,
  WattPprofTarget,
} from './types'

const CPU_PROFILE_INTERVAL_MICROS = 1000
const PPROF_CAPTURE_PACKAGE_NAME = '@platformatic/wattpm-pprof-capture'

export const PPROF_CONTROL_ERROR_CODES = {
  applicationNotFound: 'PLT_CTR_APPLICATION_NOT_FOUND',
  failedToStart: 'PLT_CTR_FAILED_TO_START_PROFILING',
  failedToStop: 'PLT_CTR_FAILED_TO_STOP_PROFILING',
  profilingAlreadyStarted: 'PLT_CTR_PROFILING_ALREADY_STARTED',
  profilingNotStarted: 'PLT_CTR_PROFILING_NOT_STARTED',
  runtimeNotFound: 'PLT_CTR_RUNTIME_NOT_FOUND',
} as const

function normalizeWorkerId(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
    return value
  }

  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value)
    if (Number.isInteger(parsed) && parsed >= 0) {
      return parsed
    }
  }

  return undefined
}

function normalizeWorkersCount(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) {
    return value
  }

  if (typeof value === 'object' && value !== null) {
    const workerConfig = value as {
      static?: unknown
      count?: unknown
      minimum?: unknown
    }

    return (
      normalizeWorkersCount(workerConfig.static) ??
      normalizeWorkersCount(workerConfig.count) ??
      normalizeWorkersCount(workerConfig.minimum)
    )
  }

  return undefined
}

export function normalizeNodeModulesSourceMaps(value: string | string[] | undefined) {
  if (!value) {
    return undefined
  }

  const modules = [
    ...new Set(
      (Array.isArray(value) ? value : [value])
        .flatMap((entry) => entry.split(','))
        .map((entry) => entry.trim())
        .filter(Boolean)
    ),
  ]
  return modules.length > 0 ? modules : undefined
}

function resolveWattPprofContext() {
  const platformatic = getGlobal()
  const applicationId = platformatic?.applicationId

  if (!applicationId) {
    return null
  }

  return {
    applicationId,
    runtimePid: process.pid,
    workerId: normalizeWorkerId(platformatic.workerId),
  }
}

export function buildPprofFilename(target: WattPprofTarget, type: PprofCaptureType) {
  const safeApplicationId = target.applicationId.replace(/[^A-Za-z0-9._-]+/g, '-')
  const workerSuffix = target.workerId === undefined ? '' : `-worker-${target.workerId}`

  return `${safeApplicationId}${workerSuffix}-${type}.pprof`
}

export function resolvePprofFilenameTarget(selection: WattPprofSelection) {
  return selection.requestedWorkerId === undefined
    ? { ...selection.targets[0], workerId: undefined }
    : selection.targets[0]
}

export function buildPprofResponseHeaders(selection: WattPprofSelection, contentType: string) {
  return {
    'cache-control': 'no-store',
    'content-type': contentType,
    'x-platformatic-application-id': selection.applicationId,
    ...(selection.requestedWorkerId !== undefined
      ? { 'x-platformatic-worker-id': `${selection.requestedWorkerId}` }
      : { 'x-platformatic-worker-count': `${selection.targets.length}` }),
  }
}

export function isAbortError(error: unknown) {
  return (
    error instanceof Error &&
    (error.name === 'AbortError' || (error as NodeJS.ErrnoException).code === 'ABORT_ERR')
  )
}

function getControlErrorCode(error: unknown) {
  return typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    typeof error.code === 'string'
    ? error.code
    : undefined
}

export function createControlStyleError(code: string, message: string) {
  return Object.assign(new Error(message), { code })
}

function getErrorMessage(error: unknown) {
  return typeof error === 'object' &&
    error !== null &&
    'message' in error &&
    typeof error.message === 'string'
    ? error.message
    : undefined
}

function getErrorCause(error: unknown) {
  return typeof error === 'object' && error !== null && 'cause' in error ? error.cause : undefined
}

function isMissingPprofCaptureDependency(error: unknown) {
  const visited = new Set<unknown>()
  let current: unknown = error

  while (current && !visited.has(current)) {
    visited.add(current)

    const code = getControlErrorCode(current)
    const message = getErrorMessage(current)
    if (
      (code === 'MODULE_NOT_FOUND' || code === 'ERR_MODULE_NOT_FOUND') &&
      message?.includes(PPROF_CAPTURE_PACKAGE_NAME)
    ) {
      return true
    }

    current = getErrorCause(current)
  }

  return false
}

export function getKnownPprofError(error: unknown): PprofKnownError | undefined {
  const code = getControlErrorCode(error)
  const message =
    error instanceof Error
      ? error.message
      : 'Failed to capture a pprof profile from the Watt runtime.'

  switch (code) {
    case PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted:
    case PPROF_CONTROL_ERROR_CODES.profilingNotStarted:
      return { code, message, statusCode: 409 }
    case PPROF_CONTROL_ERROR_CODES.runtimeNotFound:
    case PPROF_CONTROL_ERROR_CODES.applicationNotFound:
      return { code, message, statusCode: 503 }
    case PPROF_CONTROL_ERROR_CODES.failedToStart:
    case PPROF_CONTROL_ERROR_CODES.failedToStop:
      return {
        code,
        message,
        statusCode: isMissingPprofCaptureDependency(error) ? 501 : 502,
      }
    default:
      return undefined
  }
}

export function asProfilingRuntimeApiClient(value: unknown): ProfilingRuntimeApiClient {
  if (
    typeof value === 'object' &&
    value !== null &&
    'close' in value &&
    typeof value.close === 'function' &&
    'getRuntimeApplications' in value &&
    typeof value.getRuntimeApplications === 'function' &&
    'startApplicationProfiling' in value &&
    typeof value.startApplicationProfiling === 'function' &&
    'stopApplicationProfiling' in value &&
    typeof value.stopApplicationProfiling === 'function'
  ) {
    return value as ProfilingRuntimeApiClient
  }

  throw new TypeError('RuntimeApiClient does not expose the profiling control methods.')
}

export function buildPprofSessionKey(selection: WattPprofSelection, type: PprofCaptureType) {
  return `${selection.applicationId}:${type}:${selection.scopeKey}`
}

export function buildPprofStartOptions(options: {
  type: PprofCaptureType
  nodeModulesSourceMaps?: string[]
  sourceMaps?: boolean
}) {
  const startOptions: {
    type: PprofCaptureType
    intervalMicros?: number
    nodeModulesSourceMaps?: string[]
    sourceMaps?: boolean
  } = {
    type: options.type,
  }

  if (options.type === 'cpu') {
    startOptions.intervalMicros = CPU_PROFILE_INTERVAL_MICROS
  }

  if (typeof options.sourceMaps === 'boolean') {
    startOptions.sourceMaps = options.sourceMaps
  }

  if (options.nodeModulesSourceMaps && options.nodeModulesSourceMaps.length > 0) {
    startOptions.nodeModulesSourceMaps = options.nodeModulesSourceMaps
    startOptions.sourceMaps = true
  }

  return startOptions
}

export async function resolveWattPprofSelection(
  client: ProfilingRuntimeApiClient,
  requestedWorkerId: number | undefined
) {
  const context = resolveWattPprofContext()

  if (!context) {
    return null
  }

  if (requestedWorkerId !== undefined) {
    return {
      applicationId: context.applicationId,
      requestedWorkerId,
      runtimePid: context.runtimePid,
      servingWorkerId: context.workerId,
      scopeKey: `worker:${requestedWorkerId}`,
      targets: [
        {
          applicationId: context.applicationId,
          runtimePid: context.runtimePid,
          targetApplicationId: `${context.applicationId}:${requestedWorkerId}`,
          workerId: requestedWorkerId,
        },
      ],
    } satisfies WattPprofSelection
  }

  const runtimeApplications = await client.getRuntimeApplications(context.runtimePid)
  const currentApplication = runtimeApplications.applications?.find(
    (application) => application.id === context.applicationId
  ) as RuntimeApplicationWorkersShape | undefined
  const workersCount =
    normalizeWorkersCount(currentApplication?.workers) ??
    normalizeWorkersCount(currentApplication?.config?.workers) ??
    1

  if (workersCount <= 1) {
    const workerId = context.workerId
    return {
      applicationId: context.applicationId,
      runtimePid: context.runtimePid,
      servingWorkerId: context.workerId,
      scopeKey: 'all',
      targets: [
        {
          applicationId: context.applicationId,
          runtimePid: context.runtimePid,
          targetApplicationId:
            workerId === undefined ? context.applicationId : `${context.applicationId}:${workerId}`,
          workerId,
        },
      ],
    } satisfies WattPprofSelection
  }

  return {
    applicationId: context.applicationId,
    runtimePid: context.runtimePid,
    servingWorkerId: context.workerId,
    scopeKey: 'all',
    targets: Array.from({ length: workersCount }, (_, workerId) => ({
      applicationId: context.applicationId,
      runtimePid: context.runtimePid,
      targetApplicationId: `${context.applicationId}:${workerId}`,
      workerId,
    })),
  } satisfies WattPprofSelection
}
