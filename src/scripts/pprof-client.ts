import { fetchPprofStream } from '@internal/monitoring/pprof/client-http'
import { writePprofCaptureToFile } from '@internal/monitoring/pprof/download'
import { generateFlameArtifacts, resolveFlameMdFormat } from '@internal/monitoring/pprof/flame'
import type { PprofRequestTargetType } from '@internal/monitoring/pprof/types'
import path from 'path'

const ADMIN_URL = process.env.ADMIN_URL
const ADMIN_API_KEY = process.env.ADMIN_API_KEY
const PPROF_SECONDS = process.env.PPROF_SECONDS
const PPROF_GENERATE_FLAME = process.env.PPROF_GENERATE_FLAME
const PPROF_FLAME_MD_FORMAT = process.env.PPROF_FLAME_MD_FORMAT
const PPROF_WORKER_ID = process.env.PPROF_WORKER_ID
const PPROF_SOURCE_MAPS = process.env.PPROF_SOURCE_MAPS
const PPROF_NODE_MODULES_SOURCE_MAPS = process.env.PPROF_NODE_MODULES_SOURCE_MAPS
const PPROF_OUTPUT = process.env.PPROF_OUTPUT
const PPROF_USAGE =
  'Usage: tsx src/scripts/pprof-client.ts <profile[:seconds]|heap[:seconds]|heap-snapshot> [output-file]'

export function parseBooleanEnv(value: string | undefined) {
  if (value === undefined) {
    return undefined
  }

  const normalized = value.trim().toLowerCase()
  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true
  }

  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false
  }

  throw new Error(`Invalid boolean value: ${value}`)
}

export function parseBooleanEnvWithDefault(value: string | undefined, defaultValue: boolean) {
  if (value === undefined) {
    return defaultValue
  }

  return parseBooleanEnv(value) === true
}

function parseUnsignedInteger(value: string, errorMessage: string) {
  const normalized = value.trim()

  if (!/^\d+$/.test(normalized)) {
    throw new Error(errorMessage)
  }

  const parsed = Number.parseInt(normalized, 10)
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(errorMessage)
  }

  return parsed
}

export function parsePositiveIntegerEnv(
  value: string | undefined,
  envName: string,
  defaultValue: number
) {
  if (value === undefined) {
    return defaultValue
  }

  const parsed = parseUnsignedInteger(value, `${envName} must be a positive integer`)
  if (parsed <= 0) {
    throw new Error(`${envName} must be a positive integer`)
  }

  return parsed
}

export function parseNonNegativeIntegerEnv(value: string | undefined, envName: string) {
  if (value === undefined) {
    return undefined
  }

  return parseUnsignedInteger(value, `${envName} must be a non-negative integer`)
}

export function parsePprofTarget(
  value: string | undefined,
  defaultSeconds: number
):
  | { seconds: number; type: Exclude<PprofRequestTargetType, 'heap-snapshot'> }
  | {
      type: 'heap-snapshot'
    } {
  if (!value) {
    throw new Error(PPROF_USAGE)
  }

  const [type, secondsValue, ...rest] = value.split(':')

  if (rest.length > 0 || (type !== 'profile' && type !== 'heap' && type !== 'heap-snapshot')) {
    throw new Error(PPROF_USAGE)
  }

  if (type === 'heap-snapshot') {
    if (secondsValue !== undefined) {
      throw new Error(PPROF_USAGE)
    }

    return {
      type,
    }
  }

  if (secondsValue === undefined || secondsValue === '') {
    return {
      seconds: defaultSeconds,
      type,
    }
  }

  const seconds = parseUnsignedInteger(secondsValue, 'seconds must be a positive integer')
  if (seconds <= 0) {
    throw new Error('seconds must be a positive integer')
  }

  return {
    seconds,
    type,
  }
}

export function resolvePprofClientFlameMdFormat(
  generateFlame: boolean,
  target: ReturnType<typeof parsePprofTarget>,
  value: string | undefined
) {
  if (!generateFlame || target.type === 'heap-snapshot') {
    return undefined
  }

  return resolveFlameMdFormat(value)
}

function fail(message: string) {
  process.exitCode = 1
  console.error(message)
}

async function main() {
  const target = process.argv[2]
  const outputArg = process.argv[3]
  let parsedTarget: ReturnType<typeof parsePprofTarget>
  let pprofSeconds: number
  let sourceMaps: boolean | undefined
  let workerId: number | undefined
  let generateFlame: boolean

  if (!ADMIN_URL) {
    fail('Please provide ADMIN_URL')
    return
  }

  if (!ADMIN_API_KEY) {
    fail('Please provide ADMIN_API_KEY')
    return
  }

  try {
    pprofSeconds = parsePositiveIntegerEnv(PPROF_SECONDS, 'PPROF_SECONDS', 60)
    parsedTarget = parsePprofTarget(target, pprofSeconds)
    sourceMaps = parseBooleanEnv(PPROF_SOURCE_MAPS)
    workerId = parseNonNegativeIntegerEnv(PPROF_WORKER_ID, 'PPROF_WORKER_ID')
    generateFlame = parseBooleanEnvWithDefault(PPROF_GENERATE_FLAME, true)
  } catch (error) {
    process.exitCode = 1
    console.error(error instanceof Error ? error.message : error)
    return
  }

  const flameMdFormat = resolvePprofClientFlameMdFormat(
    generateFlame,
    parsedTarget,
    PPROF_FLAME_MD_FORMAT
  )

  const response =
    parsedTarget.type === 'heap-snapshot'
      ? await fetchPprofStream({
          adminUrl: ADMIN_URL,
          apiKey: ADMIN_API_KEY,
          type: 'heap-snapshot',
          workerId,
        })
      : await fetchPprofStream({
          adminUrl: ADMIN_URL,
          apiKey: ADMIN_API_KEY,
          nodeModulesSourceMaps: PPROF_NODE_MODULES_SOURCE_MAPS || undefined,
          seconds: parsedTarget.seconds,
          sourceMaps,
          type: parsedTarget.type,
          workerId,
        })

  const outputPath = outputArg
    ? path.resolve(outputArg)
    : PPROF_OUTPUT
      ? path.resolve(PPROF_OUTPUT)
      : undefined

  const { outputPath: capturedProfilePath } = await writePprofCaptureToFile(
    response.stream,
    {
      contentDisposition: response.contentDisposition,
      contentType: response.contentType,
      type: parsedTarget.type,
    },
    {
      outputPath,
    }
  )

  if (!generateFlame || parsedTarget.type === 'heap-snapshot') {
    return
  }

  await generateFlameArtifacts(capturedProfilePath, {
    env: {
      ...process.env,
      FLAME_SOURCEMAPS_DIRS: process.env.FLAME_SOURCEMAPS_DIRS || 'dist',
    },
    mdFormat: flameMdFormat,
  })
}

// Keep the CLI side effect behind a CommonJS-friendly main-module gate so tests can import
// the helpers without starting a capture.
if (require.main === module) {
  main().catch((error) => {
    process.exitCode = 1
    console.error(error)
  })
}
