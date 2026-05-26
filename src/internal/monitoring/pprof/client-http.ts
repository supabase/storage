import { Readable } from 'stream'
import type { ReadableStream as NodeReadableStream } from 'stream/web'
import type { PprofRequestTargetType } from './types'

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
    if (value === undefined) {
      continue
    }

    url.searchParams.set(key, String(value))
  }

  return url.toString()
}

async function readResponseBody(response: Response) {
  if (!response.body) {
    return ''
  }

  const reader = response.body.getReader()
  const chunks: Buffer[] = []
  let remaining = PPROF_ERROR_BODY_MAX_BYTES
  let truncated = false

  try {
    while (remaining > 0) {
      const { done, value } = await reader.read()
      if (done) {
        break
      }

      const chunk = Buffer.from(value)
      if (chunk.byteLength <= remaining) {
        chunks.push(chunk)
        remaining -= chunk.byteLength
        continue
      }

      chunks.push(chunk.subarray(0, remaining))
      remaining = 0
      truncated = true
    }
  } finally {
    if (truncated) {
      await reader.cancel().catch(() => {})
    }
  }

  const bodyText = Buffer.concat(chunks).toString('utf8').trim()
  if (!bodyText) {
    return ''
  }

  return truncated ? `: ${bodyText}… [truncated]` : `: ${bodyText}`
}

export async function fetchPprofStream(options: {
  adminUrl: string
  apiKey: string
  nodeModulesSourceMaps?: string
  seconds: number
  sourceMaps?: boolean
  type: PprofRequestTargetType
  workerId?: number
}) {
  const response = await fetch(
    resolvePprofAdminUrl(options.adminUrl, `/debug/pprof/${options.type}`, {
      nodeModulesSourceMaps: options.nodeModulesSourceMaps,
      seconds: options.seconds,
      sourceMaps: options.sourceMaps,
      workerId: options.workerId,
    }),
    {
      headers: {
        Accept: 'multipart/mixed',
        ApiKey: options.apiKey,
      },
      method: 'GET',
    }
  )

  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : ''
    throw new Error(
      `Failed to capture pprof profile: HTTP ${response.status}${statusText}${await readResponseBody(response)}`
    )
  }

  if (!response.body) {
    throw new Error('Pprof capture response did not include a response body.')
  }

  return {
    contentType: response.headers.get('content-type') ?? undefined,
    // Node's Readable.fromWeb expects the stream/web type, while fetch exposes the DOM shape.
    stream: Readable.fromWeb(response.body as unknown as NodeReadableStream),
  }
}
