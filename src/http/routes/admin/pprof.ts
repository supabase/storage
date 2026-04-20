import {
  createMultipartPprofWriter,
  waitForMultipartPprofWindow,
} from '@internal/monitoring/pprof/multipart'
import { mergeStoppedProfileBuffers } from '@internal/monitoring/pprof/profile'
import {
  asProfilingRuntimeApiClient,
  buildPprofFilename,
  buildPprofSessionKey,
  buildPprofStartOptions,
  createControlStyleError,
  getKnownPprofError,
  isAbortError,
  normalizeNodeModulesSourceMaps,
  PPROF_CONTROL_ERROR_CODES,
  resolvePprofFilenameTarget,
  resolveWattPprofSelection,
} from '@internal/monitoring/pprof/runtime'
import type {
  ActivePprofSession,
  MultipartPprofWriter,
  PprofCaptureOptions,
  PprofCaptureType,
  ProfilingRuntimeApiClient,
  WattPprofSelection,
  WattPprofTarget,
} from '@internal/monitoring/pprof/types'
import { RuntimeApiClient } from '@platformatic/control'
import { FastifyInstance, FastifyReply, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'

const CPU_PROFILE_SECONDS_DEFAULT = 10
const HEAP_PROFILE_SECONDS_DEFAULT = 10
const PPROF_SECONDS_MAX = 300
const PPROF_RESPONSE_DESCRIPTION =
  'Returns multipart/mixed. The stream starts with JSON status parts ' +
  '(`started`, `ping`, and, if capture fails after headers are sent, `error`) ' +
  'and ends with the binary profile part on success.'

const cpuProfileSchema = {
  description: `Capture a CPU pprof profile. ${PPROF_RESPONSE_DESCRIPTION}`,
  querystring: {
    type: 'object',
    properties: {
      seconds: {
        type: 'integer',
        minimum: 1,
        maximum: PPROF_SECONDS_MAX,
        default: CPU_PROFILE_SECONDS_DEFAULT,
      },
      sourceMaps: {
        type: 'boolean',
      },
      nodeModulesSourceMaps: {
        anyOf: [
          {
            type: 'string',
            minLength: 1,
          },
          {
            type: 'array',
            minItems: 1,
            items: {
              type: 'string',
              minLength: 1,
            },
          },
        ],
      },
      workerId: {
        type: 'integer',
        minimum: 0,
      },
    },
    additionalProperties: false,
  },
  tags: ['pprof'],
} as const

const heapProfileSchema = {
  description: `Capture a heap pprof profile. ${PPROF_RESPONSE_DESCRIPTION}`,
  querystring: {
    type: 'object',
    properties: {
      seconds: {
        type: 'integer',
        minimum: 1,
        maximum: PPROF_SECONDS_MAX,
        default: HEAP_PROFILE_SECONDS_DEFAULT,
      },
      sourceMaps: {
        type: 'boolean',
      },
      nodeModulesSourceMaps: {
        anyOf: [
          {
            type: 'string',
            minLength: 1,
          },
          {
            type: 'array',
            minItems: 1,
            items: {
              type: 'string',
              minLength: 1,
            },
          },
        ],
      },
      workerId: {
        type: 'integer',
        minimum: 0,
      },
    },
    additionalProperties: false,
  },
  tags: ['pprof'],
} as const

interface CpuProfileRequest extends RequestGenericInterface {
  Querystring: FromSchema<typeof cpuProfileSchema.querystring>
}

interface HeapProfileRequest extends RequestGenericInterface {
  Querystring: FromSchema<typeof heapProfileSchema.querystring>
}

const activePprofSessions = new Map<string, ActivePprofSession>()

async function stopProfilingTargets(
  client: ProfilingRuntimeApiClient,
  targets: WattPprofTarget[],
  type: PprofCaptureType
) {
  return Promise.allSettled(
    targets.map((target) =>
      client.stopApplicationProfiling(target.runtimePid, target.targetApplicationId, {
        type,
      })
    )
  )
}

async function stopPprofSession(client: ProfilingRuntimeApiClient, session: ActivePprofSession) {
  activePprofSessions.delete(session.key)
  const stopResults = await stopProfilingTargets(client, session.targets, session.type)
  return mergeStoppedProfileBuffers(stopResults)
}

async function startPprofSession(
  client: ProfilingRuntimeApiClient,
  selection: WattPprofSelection,
  options: {
    type: PprofCaptureType
    nodeModulesSourceMaps?: string[]
    sourceMaps?: boolean
  }
) {
  const key = buildPprofSessionKey(selection, options.type)

  if (activePprofSessions.has(key)) {
    throw createControlStyleError(
      PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
      selection.requestedWorkerId === undefined
        ? `Profiling is already started for application "${selection.applicationId}" (all workers).`
        : `Profiling is already started for service "${selection.applicationId}:${selection.requestedWorkerId}".`
    )
  }

  const session: ActivePprofSession = {
    ...selection,
    key,
    type: options.type,
  }
  activePprofSessions.set(key, session)

  const startOptions = buildPprofStartOptions(options)
  let startedTargets: WattPprofTarget[] = []

  try {
    const startResults = await Promise.allSettled(
      selection.targets.map((target) =>
        client.startApplicationProfiling(
          target.runtimePid,
          target.targetApplicationId,
          startOptions
        )
      )
    )
    startedTargets = selection.targets.filter(
      (_, index) => startResults[index]?.status === 'fulfilled'
    )
    const failedStart = startResults.find((result) => result.status === 'rejected')

    if (failedStart?.status === 'rejected') {
      throw failedStart.reason
    }

    return session
  } catch (error) {
    activePprofSessions.delete(key)

    if (startedTargets.length > 0) {
      await Promise.allSettled(
        startedTargets.map((target) =>
          client.stopApplicationProfiling(target.runtimePid, target.targetApplicationId, {
            type: options.type,
          })
        )
      )
    }

    throw error
  }
}

async function captureAndSendPprof(
  reply: FastifyReply,
  options: PprofCaptureOptions,
  client: ProfilingRuntimeApiClient = asProfilingRuntimeApiClient(new RuntimeApiClient())
) {
  try {
    const selection = await resolveWattPprofSelection(client, options.workerId)

    if (!selection) {
      return reply.status(501).send({
        message: 'pprof capture is only available when running under Platformatic Watt.',
      })
    }

    let session: ActivePprofSession | undefined
    let writer: MultipartPprofWriter | undefined

    try {
      session = await startPprofSession(client, selection, options)
      writer = createMultipartPprofWriter(reply, selection, options.type, options.seconds)
      await waitForMultipartPprofWindow(reply, writer, options.seconds, options.signal)

      if (!activePprofSessions.has(session.key)) {
        writer.close()
        session = undefined
        return reply
      }

      const profile = await stopPprofSession(client, session)
      session = undefined

      writer.writeBinaryPart(
        {
          'Content-Disposition': `attachment; filename="${buildPprofFilename(resolvePprofFilenameTarget(selection), options.type)}"`,
          'Content-Type': 'application/octet-stream',
        },
        profile
      )
      writer.close()
      return reply
    } catch (error) {
      if (options.signal.aborted && isAbortError(error)) {
        writer?.close()
        return reply
      }

      const knownError = getKnownPprofError(error)
      if (writer) {
        writer.writeJsonPart({
          event: 'error',
          error: {
            code: knownError?.code,
            message:
              error instanceof Error
                ? error.message
                : 'Failed to capture a pprof profile from the Watt runtime.',
            statusCode: knownError?.statusCode ?? 500,
          },
        })
        writer.close()
        return reply
      }

      if (knownError) {
        return reply.status(knownError.statusCode).send({
          message: knownError.message,
        })
      }

      throw error
    } finally {
      // onClose clears the shared session registry before it stops any in-flight captures, so
      // only the code path that still owns the registry entry should perform the final stop here.
      if (session && activePprofSessions.has(session.key)) {
        await stopPprofSession(client, session).catch(() => {})
      }
    }
  } finally {
    await client.close().catch(() => {})
  }
}

async function stopActivePprofSessions(client: ProfilingRuntimeApiClient) {
  const pendingSessions = [...activePprofSessions.values()]
  activePprofSessions.clear()

  if (pendingSessions.length === 0) {
    return
  }

  await Promise.allSettled(
    pendingSessions.map((session) => stopPprofSession(client, session).catch(() => {}))
  )
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)
  fastify.addHook('onClose', async () => {
    if (activePprofSessions.size === 0) {
      return
    }

    const client = asProfilingRuntimeApiClient(new RuntimeApiClient())

    try {
      await stopActivePprofSessions(client)
    } finally {
      await client.close().catch(() => {})
    }
  })

  fastify.get<CpuProfileRequest>(
    '/profile',
    { schema: cpuProfileSchema },
    async (request, reply) => {
      const options: PprofCaptureOptions = {
        type: 'cpu',
        seconds: request.query.seconds,
        sourceMaps: request.query.sourceMaps,
        nodeModulesSourceMaps: normalizeNodeModulesSourceMaps(request.query.nodeModulesSourceMaps),
        workerId: request.query.workerId,
        signal: request.signals.disconnect.signal,
      }

      return captureAndSendPprof(reply, options)
    }
  )

  fastify.get<HeapProfileRequest>(
    '/heap',
    { schema: heapProfileSchema },
    async (request, reply) => {
      const options: PprofCaptureOptions = {
        type: 'heap',
        seconds: request.query.seconds,
        sourceMaps: request.query.sourceMaps,
        nodeModulesSourceMaps: normalizeNodeModulesSourceMaps(request.query.nodeModulesSourceMaps),
        workerId: request.query.workerId,
        signal: request.signals.disconnect.signal,
      }

      return captureAndSendPprof(reply, options)
    }
  )
}
