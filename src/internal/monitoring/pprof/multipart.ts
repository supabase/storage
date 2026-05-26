import { randomUUID } from 'node:crypto'
import { setTimeout as wait } from 'node:timers/promises'
import type { FastifyReply } from 'fastify'
import {
  buildPprofFilename,
  buildPprofResponseHeaders,
  resolvePprofFilenameTarget,
} from './runtime'
import type { MultipartPprofWriter, PprofCaptureType, WattPprofSelection } from './types'

const PPROF_MULTIPART_PING_INTERVAL_MS = 5000
const PPROF_MULTIPART_BOUNDARY_PREFIX = 'pprof'
const PPROF_SOCKET_KEEPALIVE_INITIAL_DELAY_MS = 5000

function canWriteMultipartPprof(reply: FastifyReply) {
  return !reply.raw.destroyed && !reply.raw.writableEnded
}

function writeMultipartPprofPart(
  reply: FastifyReply,
  boundary: string,
  headers: Record<string, string>,
  body: Buffer
) {
  if (!canWriteMultipartPprof(reply)) {
    return false
  }

  const headerBlock =
    `--${boundary}\r\n` +
    Object.entries({
      ...headers,
      'Content-Length': `${body.byteLength}`,
    })
      .map(([name, value]) => `${name}: ${value}\r\n`)
      .join('') +
    '\r\n'

  try {
    reply.raw.write(headerBlock)
    reply.raw.write(body)
    reply.raw.write('\r\n')
    return true
  } catch {
    return false
  }
}

export function createMultipartPprofWriter(
  reply: FastifyReply,
  selection: WattPprofSelection,
  type: PprofCaptureType,
  seconds: number
): MultipartPprofWriter {
  const boundary = `${PPROF_MULTIPART_BOUNDARY_PREFIX}-${randomUUID()}`
  const filename = buildPprofFilename(resolvePprofFilenameTarget(selection), type)

  reply.hijack()
  reply.raw.writeHead(
    200,
    buildPprofResponseHeaders(selection, `multipart/mixed; boundary=${boundary}`)
  )

  const writer: MultipartPprofWriter = {
    boundary,
    close() {
      if (!canWriteMultipartPprof(reply)) {
        return
      }

      try {
        reply.raw.end(`--${boundary}--\r\n`)
      } catch {}
    },
    writeBinaryPart(headers, body) {
      return writeMultipartPprofPart(reply, boundary, headers, body)
    },
    writeJsonPart(payload) {
      return writeMultipartPprofPart(
        reply,
        boundary,
        {
          'Content-Type': 'application/json; charset=utf-8',
        },
        Buffer.from(JSON.stringify(payload), 'utf8')
      )
    },
  }

  writer.writeJsonPart({
    applicationId: selection.applicationId,
    event: 'started',
    filename,
    seconds,
    ...(selection.servingWorkerId !== undefined
      ? { servingWorkerId: selection.servingWorkerId }
      : {}),
    type,
    ...(selection.requestedWorkerId !== undefined
      ? { workerId: selection.requestedWorkerId }
      : { workerCount: selection.targets.length }),
  })

  return writer
}

function enablePprofSocketKeepAlive(reply: FastifyReply) {
  if (reply.raw.destroyed || reply.raw.writableEnded) {
    return
  }

  if (typeof reply.raw.socket?.setKeepAlive === 'function') {
    reply.raw.socket.setKeepAlive(true, PPROF_SOCKET_KEEPALIVE_INITIAL_DELAY_MS)
  }
}

export async function waitForMultipartPprofWindow(
  reply: FastifyReply,
  writer: MultipartPprofWriter,
  seconds: number,
  signal: AbortSignal
) {
  enablePprofSocketKeepAlive(reply)

  const keepAliveInterval = setInterval(() => {
    const wrote = writer.writeJsonPart({
      at: new Date().toISOString(),
      event: 'ping',
    })

    if (!wrote) {
      clearInterval(keepAliveInterval)
    }
  }, PPROF_MULTIPART_PING_INTERVAL_MS)
  keepAliveInterval.unref()

  try {
    await wait(seconds * 1000, undefined, { signal })
  } finally {
    clearInterval(keepAliveInterval)
  }
}
