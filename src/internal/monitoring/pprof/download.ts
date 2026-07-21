import { once } from 'events'
import fs from 'fs'
import { mkdir } from 'fs/promises'
import path from 'path'
import { PassThrough } from 'stream'
import { pipeline } from 'stream/promises'
import type { PprofRequestTargetType } from './types'

export interface MultipartPprofStartedEvent {
  applicationId: string
  event: 'started'
  filename: string
  seconds: number
  servingWorkerId?: number
  type: 'cpu' | 'heap'
  workerCount?: number
  workerId?: number
}

export interface PprofCaptureFileResult {
  outputPath: string
  startedEvent?: MultipartPprofStartedEvent
}

export interface MultipartPprofPingEvent {
  at: string
  event: 'ping'
}

export interface MultipartPprofErrorEvent {
  error: {
    code?: string
    message: string
    statusCode: number
  }
  event: 'error'
}

export type MultipartPprofJsonEvent =
  | MultipartPprofStartedEvent
  | MultipartPprofPingEvent
  | MultipartPprofErrorEvent

type MultipartHeaders = Record<string, string>

enum MultipartState {
  Body = 'body',
  Boundary = 'boundary',
  Done = 'done',
  Headers = 'headers',
}

const DEFAULT_PPROF_FILENAME = 'profile.pprof'
const DEFAULT_HEAP_SNAPSHOT_FILENAME = 'heap-snapshot.heapsnapshot'

export function extractMultipartBoundary(contentType: string | undefined) {
  if (!contentType) {
    return
  }

  const match = contentType.match(/boundary="?([^";]+)"?/i)
  return match?.[1]
}

function parseMultipartHeaders(value: string) {
  const headers: MultipartHeaders = {}

  for (const line of value.split('\r\n')) {
    const separatorIndex = line.indexOf(':')
    if (separatorIndex < 0) {
      continue
    }

    const name = line.slice(0, separatorIndex).trim().toLowerCase()
    const headerValue = line.slice(separatorIndex + 1).trim()
    if (name) {
      headers[name] = headerValue
    }
  }

  return headers
}

function parseMultipartContentLength(headers: MultipartHeaders) {
  const value = headers['content-length']
  if (!value) {
    throw new Error('Multipart pprof response is missing a Content-Length part header.')
  }

  const parsed = Number.parseInt(value, 10)
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid multipart Content-Length header: ${value}`)
  }

  return parsed
}

function parseMultipartFilename(headers: MultipartHeaders) {
  const disposition = headers['content-disposition']
  if (!disposition) {
    return
  }

  const extendedMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i)
  if (extendedMatch?.[1]) {
    try {
      return decodeURIComponent(extendedMatch[1])
    } catch {}
  }

  const quotedMatch = disposition.match(/filename="([^"]+)"/i)
  if (quotedMatch?.[1]) {
    return quotedMatch[1]
  }

  const plainMatch = disposition.match(/filename=([^;]+)/i)
  return plainMatch?.[1]?.trim()
}

function sanitizeMultipartFilename(filename: string, fallback = DEFAULT_PPROF_FILENAME) {
  const sanitized = path.posix.basename(filename.trim().replaceAll('\\', '/'))

  if (!sanitized || sanitized === '.' || sanitized === '..') {
    return fallback
  }

  return sanitized
}

async function openOutputFile(filePath: string) {
  await mkdir(path.dirname(filePath), { recursive: true })
  return fs.createWriteStream(filePath)
}

function isJsonWhitespace(byte: number) {
  return byte === 0x20 || byte === 0x09 || byte === 0x0a || byte === 0x0d
}

type HeapSnapshotChunk = Buffer | string | Uint8Array

function getChunkByteLength(chunk: HeapSnapshotChunk) {
  return typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.byteLength
}

function getChunkLength(chunk: HeapSnapshotChunk) {
  return chunk.length
}

function getChunkByte(chunk: HeapSnapshotChunk, index: number) {
  return typeof chunk === 'string' ? chunk.charCodeAt(index) : chunk[index]
}

function findFirstNonWhitespaceByte(chunk: HeapSnapshotChunk) {
  for (let index = 0; index < getChunkLength(chunk); index += 1) {
    const byte = getChunkByte(chunk, index)
    if (!isJsonWhitespace(byte)) {
      return byte
    }
  }

  return undefined
}

function findLastNonWhitespaceByte(chunk: HeapSnapshotChunk) {
  for (let index = getChunkLength(chunk) - 1; index >= 0; index -= 1) {
    const byte = getChunkByte(chunk, index)
    if (!isJsonWhitespace(byte)) {
      return byte
    }
  }

  return undefined
}

async function* validateHeapSnapshotChunks(source: AsyncIterable<Buffer | string | Uint8Array>) {
  let bytes = 0
  let firstNonWhitespace: number | undefined
  let lastNonWhitespace: number | undefined

  for await (const chunk of source) {
    const byteLength = getChunkByteLength(chunk)
    bytes += byteLength

    if (byteLength > 0) {
      if (firstNonWhitespace === undefined) {
        firstNonWhitespace = findFirstNonWhitespaceByte(chunk)
      }

      const chunkLastNonWhitespace = findLastNonWhitespaceByte(chunk)
      if (chunkLastNonWhitespace !== undefined) {
        lastNonWhitespace = chunkLastNonWhitespace
      }
    }

    yield chunk
  }

  if (bytes === 0) {
    throw new Error('Heap snapshot response was empty.')
  }

  if (firstNonWhitespace !== 0x7b || lastNonWhitespace !== 0x7d) {
    throw new Error('Heap snapshot response is not a complete JSON object.')
  }
}

async function writeHeapSnapshotStreamToFile(stream: NodeJS.ReadableStream, outputPath: string) {
  await pipeline(
    stream as AsyncIterable<Buffer | string | Uint8Array>,
    validateHeapSnapshotChunks,
    fs.createWriteStream(outputPath)
  )
}

function getErrorCause(error: unknown) {
  if (error && typeof error === 'object' && 'cause' in error) {
    return (error as { cause?: unknown }).cause
  }

  return undefined
}

function getErrorCode(error: unknown) {
  if (error && typeof error === 'object' && 'code' in error) {
    const code = (error as { code?: unknown }).code
    return typeof code === 'string' ? code : undefined
  }

  return undefined
}

function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : undefined
}

function isTerminatedMultipartStreamError(error: unknown): boolean {
  const cause = getErrorCause(error)

  if (getErrorCode(cause) === 'UND_ERR_SOCKET') {
    return true
  }

  if (error instanceof TypeError && error.message === 'terminated') {
    return true
  }

  const causeMessage = getErrorMessage(cause)?.toLowerCase()
  if (causeMessage?.includes('other side closed') || causeMessage?.includes('socket closed')) {
    return true
  }

  return false
}

function buildServingWorkerDetail(servingWorkerId: number | undefined) {
  return servingWorkerId === undefined ? '' : ` Serving worker: ${servingWorkerId}.`
}

function buildInterruptedCaptureMessage(options: {
  lastHeartbeatAt?: string
  startedEvent: MultipartPprofStartedEvent
}) {
  const captureLabel =
    `${options.startedEvent.applicationId} ` +
    `(${options.startedEvent.type}, ${options.startedEvent.seconds}s)`
  const lastHeartbeatDetail = options.lastHeartbeatAt
    ? ` Last heartbeat arrived at ${options.lastHeartbeatAt}.`
    : ''
  const servingWorkerDetail = buildServingWorkerDetail(options.startedEvent.servingWorkerId)

  return (
    `Pprof capture stream ended before the profile was delivered for ${captureLabel}.` +
    lastHeartbeatDetail +
    servingWorkerDetail +
    ' The connection died mid-capture, usually because a load balancer ' +
    'closed the long-running response or the serving worker exited. ' +
    'Use a shorter capture.'
  )
}

function createInterruptedCaptureError(
  options: {
    lastHeartbeatAt?: string
    startedEvent: MultipartPprofStartedEvent
  },
  cause?: unknown
) {
  return cause === undefined
    ? new Error(buildInterruptedCaptureMessage(options))
    : new Error(buildInterruptedCaptureMessage(options), { cause })
}

export async function writeMultipartPprofToFile(
  stream: NodeJS.ReadableStream,
  contentType: string | undefined,
  options?: {
    outputPath?: string
  }
): Promise<PprofCaptureFileResult> {
  const boundary = extractMultipartBoundary(contentType)
  if (!boundary) {
    throw new Error('Expected a multipart/mixed pprof response with a boundary parameter.')
  }

  const boundaryLine = `--${boundary}`
  const parserState = { value: MultipartState.Boundary as MultipartState }
  let buffer = Buffer.alloc(0)
  let currentHeaders: MultipartHeaders = {}
  let currentLength = 0
  let jsonChunks: Buffer[] = []
  let outputPath = options?.outputPath ? path.resolve(options.outputPath) : undefined
  let profileBodyStream: PassThrough | undefined
  let profileOutputPipeline: Promise<void> | undefined
  let receivedProfile = false
  let startedEvent: MultipartPprofStartedEvent | undefined
  let lastHeartbeatAt: string | undefined

  const emitJsonPart = async () => {
    const payload = JSON.parse(
      Buffer.concat(jsonChunks).toString('utf8')
    ) as MultipartPprofJsonEvent
    jsonChunks = []

    if (payload.event === 'started') {
      startedEvent = payload
      const servingWorkerDetail = buildServingWorkerDetail(payload.servingWorkerId)
      console.log(
        `Capture started for ${payload.applicationId} (${payload.type}, ${payload.seconds}s).${servingWorkerDetail}`
      )
      return
    }

    if (payload.event === 'ping') {
      lastHeartbeatAt = payload.at
      console.log(`Capture still running at ${payload.at}.`)
      return
    }

    throw new Error(
      payload.error.code
        ? `[${payload.error.code}] ${payload.error.message}`
        : payload.error.message
    )
  }

  const openProfileOutput = async () => {
    if (profileBodyStream) {
      return profileBodyStream
    }

    const filename = sanitizeMultipartFilename(
      parseMultipartFilename(currentHeaders) ?? startedEvent?.filename ?? DEFAULT_PPROF_FILENAME
    )

    outputPath ??= path.resolve(process.cwd(), 'dist', filename)
    const outputFile = await openOutputFile(outputPath)
    profileBodyStream = new PassThrough()
    profileOutputPipeline = pipeline(profileBodyStream, outputFile)
    void profileOutputPipeline.catch(() => {})
    return profileBodyStream
  }

  const getProfileOutputPipeline = () => {
    if (!profileOutputPipeline) {
      throw new Error('Profile output pipeline was not initialized.')
    }

    return profileOutputPipeline
  }

  const writeProfileChunk = async (chunk: Buffer) => {
    const profileBody = await openProfileOutput()
    if (profileBody.write(chunk)) {
      return
    }

    await Promise.race([
      once(profileBody, 'drain').then(() => undefined),
      getProfileOutputPipeline(),
    ])
  }

  const finishProfileOutput = async () => {
    if (!profileBodyStream) {
      return
    }

    profileBodyStream.end()
    await getProfileOutputPipeline()
    profileBodyStream = undefined
    profileOutputPipeline = undefined
  }

  const abortProfileOutput = async () => {
    if (!profileBodyStream) {
      return
    }

    profileBodyStream.destroy()
    await profileOutputPipeline?.catch(() => {})
    profileBodyStream = undefined
    profileOutputPipeline = undefined
  }

  const consumeBoundaryLine = () => {
    const lineEnd = buffer.indexOf('\r\n')
    if (lineEnd < 0) {
      return false
    }

    const line = buffer.subarray(0, lineEnd).toString('latin1')
    buffer = buffer.subarray(lineEnd + 2)

    if (line === `${boundaryLine}--`) {
      parserState.value = MultipartState.Done
      return true
    }

    if (line === boundaryLine) {
      parserState.value = MultipartState.Headers
      return true
    }

    throw new Error(`Unexpected multipart boundary line: ${line}`)
  }

  const consumeHeaders = () => {
    const headersEnd = buffer.indexOf('\r\n\r\n')
    if (headersEnd < 0) {
      return false
    }

    currentHeaders = parseMultipartHeaders(buffer.subarray(0, headersEnd).toString('latin1'))
    currentLength = parseMultipartContentLength(currentHeaders)
    buffer = buffer.subarray(headersEnd + 4)
    parserState.value = MultipartState.Body
    return true
  }

  const consumeJsonBody = async () => {
    if (buffer.length < currentLength + 2) {
      return false
    }

    jsonChunks.push(buffer.subarray(0, currentLength))

    if (buffer.subarray(currentLength, currentLength + 2).toString('latin1') !== '\r\n') {
      throw new Error('Malformed multipart pprof response: missing CRLF after JSON part body.')
    }

    buffer = buffer.subarray(currentLength + 2)
    await emitJsonPart()
    parserState.value = MultipartState.Boundary
    return true
  }

  const consumeProfileBody = async () => {
    const bytesToWrite = Math.min(currentLength, buffer.length)

    if (bytesToWrite > 0) {
      const chunk = buffer.subarray(0, bytesToWrite)
      buffer = buffer.subarray(bytesToWrite)
      currentLength -= bytesToWrite
      await writeProfileChunk(chunk)
    }

    if (currentLength > 0) {
      return false
    }

    if (buffer.length < 2) {
      return false
    }

    if (buffer.subarray(0, 2).toString('latin1') !== '\r\n') {
      throw new Error('Malformed multipart pprof response: missing CRLF after binary profile part.')
    }

    buffer = buffer.subarray(2)
    await finishProfileOutput()
    receivedProfile = true
    parserState.value = MultipartState.Boundary
    return true
  }

  try {
    try {
      for await (const chunk of stream) {
        buffer =
          buffer.length === 0
            ? Buffer.from(chunk as Uint8Array)
            : Buffer.concat([buffer, chunk as Uint8Array])

        let shouldContinueParsing = true
        while (shouldContinueParsing) {
          const currentState = parserState.value

          if (currentState === MultipartState.Done) {
            break
          }

          if (currentState === MultipartState.Boundary) {
            if (!consumeBoundaryLine()) {
              shouldContinueParsing = false
            }
            continue
          }

          if (currentState === MultipartState.Headers) {
            if (!consumeHeaders()) {
              shouldContinueParsing = false
            }
            continue
          }

          const contentTypeHeader = currentHeaders['content-type']?.toLowerCase() ?? ''
          const consumed = contentTypeHeader.startsWith('application/json')
            ? await consumeJsonBody()
            : await consumeProfileBody()

          if (!consumed) {
            shouldContinueParsing = false
          }
        }

        if (parserState.value === MultipartState.Done) {
          break
        }
      }
    } catch (error) {
      if (startedEvent && !receivedProfile && isTerminatedMultipartStreamError(error)) {
        throw createInterruptedCaptureError(
          {
            lastHeartbeatAt,
            startedEvent,
          },
          error
        )
      }

      throw error
    }
  } finally {
    await abortProfileOutput()
  }

  if (parserState.value !== MultipartState.Done) {
    if (startedEvent && !receivedProfile) {
      throw createInterruptedCaptureError({
        lastHeartbeatAt,
        startedEvent,
      })
    }

    throw new Error('Multipart pprof response ended before the closing boundary.')
  }

  if (!receivedProfile || !outputPath) {
    throw new Error('Multipart pprof response completed without a profile part.')
  }

  console.log(`Saved pprof profile to ${outputPath}.`)

  return {
    outputPath,
    startedEvent,
  }
}

export async function writePprofCaptureToFile(
  stream: NodeJS.ReadableStream,
  response: {
    contentDisposition?: string
    contentType?: string
    type: PprofRequestTargetType
  },
  options?: {
    outputPath?: string
  }
): Promise<PprofCaptureFileResult> {
  if (response.type !== 'heap-snapshot') {
    return writeMultipartPprofToFile(stream, response.contentType, options)
  }

  const filename = sanitizeMultipartFilename(
    parseMultipartFilename({
      'content-disposition': response.contentDisposition ?? '',
    }) ?? DEFAULT_HEAP_SNAPSHOT_FILENAME,
    DEFAULT_HEAP_SNAPSHOT_FILENAME
  )
  const outputPath = options?.outputPath
    ? path.resolve(options.outputPath)
    : path.resolve(process.cwd(), 'dist', filename)

  await mkdir(path.dirname(outputPath), { recursive: true })
  await writeHeapSnapshotStreamToFile(stream, outputPath)
  console.log(`Saved pprof capture to ${outputPath}.`)

  return {
    outputPath,
  }
}
