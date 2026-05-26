export type PprofCaptureType = 'cpu' | 'heap'
export type PprofRequestTargetType = 'heap' | 'profile'

export interface WattPprofTarget {
  applicationId: string
  runtimePid: number
  targetApplicationId: string
  workerId?: number
}

export interface WattPprofSelection {
  applicationId: string
  requestedWorkerId?: number
  runtimePid: number
  servingWorkerId?: number
  scopeKey: string
  targets: WattPprofTarget[]
}

export interface ActivePprofSession extends WattPprofSelection {
  key: string
  type: PprofCaptureType
}

export interface PprofCaptureOptions {
  signal: AbortSignal
  sourceMaps?: boolean
  seconds: number
  type: PprofCaptureType
  nodeModulesSourceMaps?: string[]
  workerId?: number
}

export interface PprofKnownError {
  code?: string
  message: string
  statusCode: number
}

export interface MultipartPprofWriter {
  boundary: string
  writeBinaryPart: (headers: Record<string, string>, body: Buffer) => boolean
  writeJsonPart: (payload: unknown) => boolean
  close: () => void
}

export interface ProfilingRuntimeApiClient {
  close(): Promise<void>
  getRuntimeApplications(pid: number): Promise<{
    applications?: Array<{
      id?: string
      workers?: unknown
      config?: {
        workers?: unknown
      }
    }>
  }>
  startApplicationProfiling(
    pid: number,
    applicationId: string,
    options?: {
      type: PprofCaptureType
      intervalMicros?: number
      nodeModulesSourceMaps?: string[]
      sourceMaps?: boolean
    }
  ): Promise<unknown>
  stopApplicationProfiling(
    pid: number,
    applicationId: string,
    options?: {
      type: PprofCaptureType
    }
  ): Promise<ArrayBuffer>
}

export type RuntimeApplicationWorkersShape = {
  workers?: unknown
  config?: {
    workers?: unknown
  }
}
