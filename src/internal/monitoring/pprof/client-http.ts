import { Readable } from 'node:stream'
import type { ReadableStream as NodeReadableStream } from 'node:stream/web'
import type { PprofRequestTargetType } from './download'
import type { ProfileClass, ProfileKind } from './store-key'

export interface PprofArchivedProfile {
  key: string
  class: ProfileClass
  kind: ProfileKind
  reason: string
  startedAt: string
  durationSeconds: number
  hostname: string
  applicationId?: string
  workerId?: string
  processId: number
  build: string
  size?: number
  etag?: string
}

export interface PprofArchivedProfileList {
  profiles: PprofArchivedProfile[]
  cursor?: string
}

export interface PprofCaptureTriggerResult {
  scheduled: true
  class: 'manual'
  kind: ProfileKind
  message: string
}

const PPROF_ERROR_BODY_MAX_BYTES = 4 * 1024

type PprofQueryValue = boolean | number | string | undefined

export function resolvePprofAdminUrl(
  baseUrl: string,
  requestPath: string,
  params?: Record<string, PprofQueryValue>
) {
  const url = new URL(baseUrl)
  const normalizedBasePath = url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`
  const normalizedRequestPath = requestPath.replace(/^\/+/, '')

  url.hash = ''
  url.search = ''
  url.pathname =
    normalizedBasePath === '/'
      ? `/${normalizedRequestPath}`
      : `${normalizedBasePath}${normalizedRequestPath}`

  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined) url.searchParams.set(key, String(value))
  }

  return url.toString()
}

async function readResponseBody(response: Response) {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const chunks: Buffer[] = []
  let remaining = PPROF_ERROR_BODY_MAX_BYTES
  let truncated = false
  let complete = false

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) {
        complete = true
        break
      }
      if (remaining === 0) {
        truncated = true
        break
      }
      const chunk = Buffer.from(value)
      const available = remaining
      chunks.push(chunk.subarray(0, available))
      remaining -= Math.min(chunk.byteLength, available)
      if (chunk.byteLength > available) truncated = true
      if (truncated) break
    }
  } finally {
    if (!complete) await reader.cancel().catch(() => {})
  }

  const body = Buffer.concat(chunks).toString('utf8').trim()
  return body ? `: ${body}${truncated ? '… [truncated]' : ''}` : ''
}

async function request(options: {
  adminUrl: string
  apiKey: string
  path: string
  params?: Record<string, PprofQueryValue>
  accept: string
}) {
  const response = await fetch(
    resolvePprofAdminUrl(options.adminUrl, options.path, options.params),
    {
      headers: { Accept: options.accept, ApiKey: options.apiKey },
      method: 'GET',
      redirect: 'error',
    }
  )

  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : ''
    throw new Error(
      `Pprof admin request failed: HTTP ${response.status}${statusText}${await readResponseBody(response)}`
    )
  }
  return response
}

function asStream(response: Response) {
  if (!response.body) throw new Error('Pprof response did not include a response body.')
  return {
    contentDisposition: response.headers.get('content-disposition') ?? undefined,
    stream: Readable.fromWeb(response.body as unknown as NodeReadableStream),
  }
}

export async function fetchPprofStream(options: {
  adminUrl: string
  apiKey: string
  type: Extract<PprofRequestTargetType, 'heap-snapshot'>
}) {
  return asStream(
    await request({
      adminUrl: options.adminUrl,
      apiKey: options.apiKey,
      path: `/debug/pprof/${options.type}`,
      accept: 'application/json',
    })
  )
}

export async function triggerPprofCapture(options: {
  adminUrl: string
  apiKey: string
  type: ProfileKind
  seconds: number
}) {
  const response = await request({
    adminUrl: options.adminUrl,
    apiKey: options.apiKey,
    path: `/debug/pprof/${options.type === 'cpu' ? 'profile' : 'heap'}`,
    params: { seconds: options.seconds },
    accept: 'application/json',
  })
  return (await response.json()) as PprofCaptureTriggerResult
}

export async function fetchArchivedProfiles(options: {
  adminUrl: string
  apiKey: string
  class: ProfileClass
  kind?: ProfileKind
  date?: string
  limit?: number
  cursor?: string
}) {
  const response = await request({
    adminUrl: options.adminUrl,
    apiKey: options.apiKey,
    path: '/debug/pprof/profiles',
    params: {
      class: options.class,
      kind: options.kind,
      date: options.date,
      limit: options.limit,
      cursor: options.cursor,
    },
    accept: 'application/json',
  })
  return (await response.json()) as PprofArchivedProfileList
}

export async function downloadArchivedProfile(options: {
  adminUrl: string
  apiKey: string
  key: string
}) {
  return asStream(
    await request({
      adminUrl: options.adminUrl,
      apiKey: options.apiKey,
      path: '/debug/pprof/profiles/download',
      params: { key: options.key },
      accept: 'application/gzip',
    })
  )
}
