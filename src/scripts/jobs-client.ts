type JobsAction = 'backup' | 'list' | 'restore'
type JobsGroupBy = 'summary' | 'tenant'
type JobsSource = 'backup' | 'job'

interface JobsClientConfig {
  adminApiKey: string
  adminUrl: string
  eventTypes?: string[]
  groupBy: JobsGroupBy
  limit?: number
  queueName?: string
  source: JobsSource
  tenantRefs?: string[]
}

const ADMIN_URL = process.env.ADMIN_URL
const ADMIN_API_KEY = process.env.ADMIN_API_KEY
const JOBS_SOURCE = process.env.JOBS_SOURCE
const JOBS_GROUP_BY = process.env.JOBS_GROUP_BY
const JOBS_QUEUE_NAME = process.env.JOBS_QUEUE_NAME
const JOBS_EVENT_TYPES = process.env.JOBS_EVENT_TYPES
const JOBS_TENANT_REFS = process.env.JOBS_TENANT_REFS
const JOBS_LIMIT = process.env.JOBS_LIMIT

export function parseJobsCsv(value: string | undefined) {
  if (!value) {
    return undefined
  }

  const normalized = Array.from(
    new Set(
      value
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
    )
  )

  return normalized.length > 0 ? normalized : undefined
}

function parseUnsignedInteger(value: string, errorMessage: string) {
  const normalized = value.trim()

  if (!/^\d+$/.test(normalized)) {
    throw new Error(errorMessage)
  }

  const parsed = Number.parseInt(normalized, 10)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(errorMessage)
  }

  return parsed
}

export function resolveJobsAdminUrl(
  baseUrl: string,
  requestPath: string,
  query?: Record<string, string | undefined>
) {
  const url = new URL(`${baseUrl.replace(/\/+$/, '')}/${requestPath.replace(/^\/+/, '')}`)

  for (const [key, value] of Object.entries(query ?? {})) {
    if (value !== undefined) {
      url.searchParams.set(key, value)
    }
  }

  return url
}

export function parseJobsConfig(env: NodeJS.ProcessEnv): JobsClientConfig | string {
  const source = (env.JOBS_SOURCE?.trim() || 'job') as JobsSource
  const groupBy = (env.JOBS_GROUP_BY?.trim() || 'summary') as JobsGroupBy

  if (!env.ADMIN_URL) {
    return 'Please provide ADMIN_URL'
  }

  if (!env.ADMIN_API_KEY) {
    return 'Please provide ADMIN_API_KEY'
  }

  if (source !== 'job' && source !== 'backup') {
    return 'JOBS_SOURCE must be either job or backup'
  }

  if (groupBy !== 'summary' && groupBy !== 'tenant') {
    return 'JOBS_GROUP_BY must be either summary or tenant'
  }

  try {
    return {
      adminApiKey: env.ADMIN_API_KEY,
      adminUrl: env.ADMIN_URL,
      queueName: env.JOBS_QUEUE_NAME?.trim() || undefined,
      eventTypes: parseJobsCsv(env.JOBS_EVENT_TYPES),
      tenantRefs: parseJobsCsv(env.JOBS_TENANT_REFS),
      source,
      groupBy,
      limit: env.JOBS_LIMIT
        ? parseUnsignedInteger(env.JOBS_LIMIT, 'JOBS_LIMIT must be a positive integer')
        : undefined,
    }
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}

export function buildJobsRequest(action: JobsAction, config: JobsClientConfig) {
  const headers = new Headers({
    ApiKey: config.adminApiKey,
  })

  if (action === 'list') {
    return {
      method: 'GET',
      url: resolveJobsAdminUrl(config.adminUrl, '/queue/overflow', {
        source: config.source,
        groupBy: config.groupBy,
        name: config.queueName,
        eventTypes: config.eventTypes?.join(','),
        tenantRefs: config.tenantRefs?.join(','),
        limit: config.limit?.toString(),
      }),
      headers,
      body: undefined,
    }
  }

  headers.set('Content-Type', 'application/json')

  return {
    method: 'POST',
    url: resolveJobsAdminUrl(
      config.adminUrl,
      action === 'backup' ? '/queue/overflow/backup' : '/queue/overflow/restore'
    ),
    headers,
    body: JSON.stringify({
      name: config.queueName,
      eventTypes: config.eventTypes,
      tenantRefs: config.tenantRefs,
      limit: config.limit,
    }),
  }
}

async function assertJsonResponse(response: Response, context: string) {
  if (response.ok) {
    return response.json()
  }

  const body = await response.text()
  const details = body ? `: ${body}` : ''

  throw new Error(`${context} failed with ${response.status} ${response.statusText}${details}`)
}

function fail(message: string) {
  process.exitCode = 1
  console.error(message)
  return false
}

export async function main(
  env: NodeJS.ProcessEnv = process.env,
  argv: string[] = process.argv
): Promise<boolean> {
  const action = argv[2]

  if (action !== 'list' && action !== 'backup' && action !== 'restore') {
    return fail('Please provide an action: list, backup, or restore')
  }

  const config = parseJobsConfig(env)
  if (typeof config === 'string') {
    return fail(config)
  }

  try {
    const request = buildJobsRequest(action, config)
    const response = await fetch(request.url, {
      method: request.method,
      headers: request.headers,
      body: request.body,
    })

    const data = await assertJsonResponse(response, `${action.toUpperCase()} ${request.url}`)
    console.log(JSON.stringify(data, null, 2))
    return true
  } catch (error) {
    return fail(error instanceof Error ? error.message : String(error))
  }
}

if (require.main === module) {
  main({
    ADMIN_URL,
    ADMIN_API_KEY,
    JOBS_SOURCE,
    JOBS_GROUP_BY,
    JOBS_QUEUE_NAME,
    JOBS_EVENT_TYPES,
    JOBS_TENANT_REFS,
    JOBS_LIMIT,
  }).catch((error) => {
    process.exitCode = 1
    console.error(error)
  })
}
