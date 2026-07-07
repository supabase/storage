import {
  createMultipartPprofWriter,
  waitForMultipartPprofWindow,
} from '@internal/monitoring/pprof/multipart'
import { mergeStoppedProfileBuffers } from '@internal/monitoring/pprof/profile'
import {
  asProfilingRuntimeApiClient,
  buildHeapSnapshotResponseHeaders,
  buildPprofFilename,
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
  resolveWattPprofSelectionForWorkerIds,
} from '@internal/monitoring/pprof/runtime'
import type {
  ActivePprofSession,
  HeapSnapshotStream,
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
import { registerApiKeyAuth } from '../../plugins/apikey'

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

const heapSnapshotSchema = {
  description:
    'Capture a full V8 heap snapshot for one Watt worker. Returns a streamed .heapsnapshot file.',
  querystring: {
    type: 'object',
    properties: {
      workerId: {
        type: 'integer',
        minimum: 0,
      },
    },
    additionalProperties: false,
  },
  tags: ['pprof'],
} as const

interface HeapSnapshotRequest extends RequestGenericInterface {
  Querystring: FromSchema<typeof heapSnapshotSchema.querystring>
}

const activePprofSessions = new Map<string, ActivePprofSession>()

interface ActiveHeapSnapshotSession {
  closeClient: () => Promise<void>
  key: string
  stream?: HeapSnapshotStream
}

const activeHeapSnapshotSessions = new Map<string, ActiveHeapSnapshotSession>()

function createAbortError(): Error & { code: string } {
  const error = new Error('The operation was aborted.') as Error & { code: string }
  error.name = 'AbortError'
  error.code = 'ABORT_ERR'
  return error
}

function formatWorkerIds(workerIds: number[]) {
  return workerIds.length === 0 ? '' : ` Available workerIds: ${workerIds.join(', ')}.`
}

function getSelectionWorkerIds(selection: WattPprofSelection) {
  return [
    ...new Set(
      selection.targets
        .map((target) => target.workerId)
        .filter((workerId): workerId is number => workerId !== undefined)
    ),
  ]
}

function buildHeapSnapshotSessionKey(selection: WattPprofSelection, target: WattPprofTarget) {
  return `${selection.applicationId}:heap-snapshot:${target.targetApplicationId}`
}

function createHeapSnapshotAlreadyStartedError(
  selection: WattPprofSelection,
  target: WattPprofTarget
) {
  return createControlStyleError(
    PPROF_CONTROL_ERROR_CODES.profilingAlreadyStarted,
    `Heap snapshot capture is already running for service "${selection.applicationId}:${target.workerId}".`
  )
}

async function withProfilingClient(
  reply: FastifyReply,
  options: {
    client?: ProfilingRuntimeApiClient
    notAvailableMessage: string
    workerId?: number
  },
  handler: (
    client: ProfilingRuntimeApiClient,
    selection: WattPprofSelection,
    controls: {
      closeClient: () => Promise<void>
      keepClientOpen: () => void
    }
  ) => Promise<FastifyReply>
) {
  const client = options.client ?? asProfilingRuntimeApiClient(new RuntimeApiClient())
  let shouldCloseClient = true
  let closePromise: Promise<void> | undefined

  const closeClient = () => {
    closePromise ??= client.close().catch(() => {})
    return closePromise
  }

  try {
    const selection = await resolveWattPprofSelection(client, options.workerId)

    if (!selection) {
      return reply.status(501).send({
        message: options.notAvailableMessage,
      })
    }

    return await handler(client, selection, {
      closeClient,
      keepClientOpen() {
        shouldCloseClient = false
      },
    })
  } catch (error) {
    const knownError = getKnownPprofError(error)

    if (knownError) {
      return reply.status(knownError.statusCode).send({
        message: knownError.message,
      })
    }

    throw error
  } finally {
    if (shouldCloseClient) {
      await closeClient()
    }
  }
}

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
    await stopProfilingTargets(client, startedTargets, options.type)
    throw error
  }
}

async function startPprofSessionWithLiveWorkerRetry(
  client: ProfilingRuntimeApiClient,
  selection: WattPprofSelection,
  options: PprofCaptureOptions
) {
  try {
    return await startPprofSession(client, selection, options)
  } catch (error) {
    if (options.signal.aborted || selection.requestedWorkerId !== undefined) {
      throw error
    }

    const liveWorkerSelection = resolveWattPprofSelectionForWorkerIds(
      selection,
      resolveRuntimeWorkerIdsFromError(error)
    )

    if (!liveWorkerSelection) {
      throw error
    }

    return startPprofSession(client, liveWorkerSelection, options)
  }
}

async function captureAndSendPprof(
  reply: FastifyReply,
  options: PprofCaptureOptions,
  client: ProfilingRuntimeApiClient = asProfilingRuntimeApiClient(new RuntimeApiClient())
) {
  return withProfilingClient(
    reply,
    {
      client,
      notAvailableMessage: 'pprof capture is only available when running under Platformatic Watt.',
      workerId: options.workerId,
    },
    async (client, selection) => {
      let selectedTargets = selection
      let session: ActivePprofSession | undefined
      let writer: MultipartPprofWriter | undefined

      try {
        session = await startPprofSessionWithLiveWorkerRetry(client, selectedTargets, options)
        selectedTargets = session
        writer = createMultipartPprofWriter(reply, selectedTargets, options.type, options.seconds)
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
            'Content-Disposition': `attachment; filename="${buildPprofFilename(resolvePprofFilenameTarget(selectedTargets), options.type)}"`,
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
    }
  )
}

function cleanupHeapSnapshotSession(
  session: ActiveHeapSnapshotSession,
  abortSignal: AbortSignal,
  abortHandler: () => void
) {
  activeHeapSnapshotSessions.delete(session.key)
  abortSignal.removeEventListener('abort', abortHandler)
  void session.closeClient()
}

function closeHeapSnapshotSessionWhenStreamEnds(
  reply: FastifyReply,
  session: ActiveHeapSnapshotSession,
  abortSignal: AbortSignal,
  abortHandler: () => void
) {
  let cleaned = false
  const cleanup = () => {
    if (cleaned) {
      return
    }

    cleaned = true
    cleanupHeapSnapshotSession(session, abortSignal, abortHandler)
  }

  session.stream?.once('end', cleanup)
  session.stream?.once('error', cleanup)
  session.stream?.once('close', cleanup)
  reply.raw.once('close', cleanup)
}

async function captureAndSendHeapSnapshot(
  reply: FastifyReply,
  options: {
    signal: AbortSignal
    workerId?: number
  },
  client: ProfilingRuntimeApiClient = asProfilingRuntimeApiClient(new RuntimeApiClient())
) {
  return withProfilingClient(
    reply,
    {
      client,
      notAvailableMessage:
        'heap snapshot capture is only available when running under Platformatic Watt.',
      workerId: options.workerId,
    },
    async (client, selection, controls) => {
      if (options.workerId === undefined) {
        return reply.status(400).send({
          message:
            'Full heap snapshots are per V8 isolate; pass workerId to capture exactly one worker.' +
            formatWorkerIds(getSelectionWorkerIds(selection)),
        })
      }

      const target = selection.targets[0]
      const session: ActiveHeapSnapshotSession = {
        closeClient: controls.closeClient,
        key: buildHeapSnapshotSessionKey(selection, target),
      }

      if (activeHeapSnapshotSessions.has(session.key)) {
        throw createHeapSnapshotAlreadyStartedError(selection, target)
      }

      activeHeapSnapshotSessions.set(session.key, session)

      const abortHandler = () => {
        session.stream?.destroy(createAbortError())
        void controls.closeClient()
      }
      options.signal.addEventListener('abort', abortHandler, { once: true })

      try {
        const snapshot = await client.takeApplicationHeapSnapshot(
          target.runtimePid,
          target.targetApplicationId
        )
        session.stream = snapshot

        if (options.signal.aborted) {
          snapshot.destroy(createAbortError())
          return reply
        }

        closeHeapSnapshotSessionWhenStreamEnds(reply, session, options.signal, abortHandler)
        controls.keepClientOpen()

        return reply.headers(buildHeapSnapshotResponseHeaders(selection, target)).send(snapshot)
      } catch (error) {
        const workerIds = resolveRuntimeWorkerIdsFromError(error)

        if (workerIds && workerIds.length > 0) {
          return reply.status(400).send({
            message: `Worker ${options.workerId} is not available.${formatWorkerIds(workerIds)}`,
          })
        }

        if (options.signal.aborted) {
          return reply
        }

        throw error
      } finally {
        if (!session.stream || options.signal.aborted) {
          cleanupHeapSnapshotSession(session, options.signal, abortHandler)
        }
      }
    }
  )
}

async function stopActiveHeapSnapshotSessions() {
  const pendingSessions = [...activeHeapSnapshotSessions.values()]
  activeHeapSnapshotSessions.clear()

  await Promise.allSettled(
    pendingSessions.map(async (session) => {
      session.stream?.destroy(createAbortError())
      await session.closeClient()
    })
  )
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

async function stopActiveSessions() {
  const pendingSnapshotCount = activeHeapSnapshotSessions.size
  const pendingPprofCount = activePprofSessions.size

  if (pendingSnapshotCount > 0) {
    await stopActiveHeapSnapshotSessions()
  }

  if (pendingPprofCount === 0) {
    return
  }

  const client = asProfilingRuntimeApiClient(new RuntimeApiClient())

  try {
    await stopActivePprofSessions(client)
  } finally {
    await client.close().catch(() => {})
  }
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)
  fastify.addHook('onClose', async () => {
    if (activePprofSessions.size === 0 && activeHeapSnapshotSessions.size === 0) {
      return
    }

    await stopActiveSessions()
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

  fastify.get<HeapSnapshotRequest>(
    '/heap-snapshot',
    { schema: heapSnapshotSchema },
    async (request, reply) => {
      return captureAndSendHeapSnapshot(reply, {
        signal: request.signals.disconnect.signal,
        workerId: request.query.workerId,
      })
    }
  )
}
