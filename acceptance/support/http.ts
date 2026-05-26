import { getAcceptanceConfig, joinUrl } from './config'
import { createAcceptanceHeaders } from './headers'

export { createAcceptanceHeaders, withAcceptanceHeaders } from './headers'

export interface HttpRequestOptions {
  body?: BodyInit | Record<string, unknown>
  expectedStatus?: number | number[]
  headers?: Record<string, string>
  token?: string
}

export interface HttpResponse<T = unknown> {
  body: string
  headers: Headers
  json: T | undefined
  status: number
  url: string
}

export class AcceptanceHttpClient {
  constructor(private readonly baseUrl: string) {}

  async request<T = unknown>(
    method: string,
    route: string,
    options: HttpRequestOptions = {}
  ): Promise<HttpResponse<T>> {
    const url = joinUrl(this.baseUrl, route)
    const headers = createAcceptanceHeaders(options.headers)

    if (options.token) {
      headers.set('authorization', `Bearer ${options.token}`)
    }

    let body = options.body
    if (body && isPlainObject(body)) {
      headers.set('content-type', headers.get('content-type') ?? 'application/json')
      body = JSON.stringify(body)
    }

    const response = await fetch(url, {
      body,
      headers,
      method,
    })
    const text = await response.text()
    const parsed = parseJson<T>(text)
    const expected = options.expectedStatus

    if (expected !== undefined && !statusMatches(response.status, expected)) {
      throw new Error(
        [
          `Unexpected HTTP status for ${method} ${url}`,
          `expected: ${Array.isArray(expected) ? expected.join(', ') : expected}`,
          `received: ${response.status}`,
          `body: ${text}`,
        ].join('\n')
      )
    }

    return {
      body: text,
      headers: response.headers,
      json: parsed,
      status: response.status,
      url,
    }
  }
}

export function createRestClient() {
  return new AcceptanceHttpClient(getAcceptanceConfig().baseUrl)
}

export function createAdminClient() {
  const { adminUrl } = getAcceptanceConfig()
  if (!adminUrl) {
    throw new Error('ACCEPTANCE_ADMIN_URL is required for admin acceptance tests')
  }

  return new AcceptanceHttpClient(adminUrl)
}

function statusMatches(status: number, expected: number | number[]) {
  return Array.isArray(expected) ? expected.includes(status) : status === expected
}

function parseJson<T>(text: string): T | undefined {
  if (!text) {
    return undefined
  }

  try {
    return JSON.parse(text) as T
  } catch {
    return undefined
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null) {
    return false
  }

  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}
