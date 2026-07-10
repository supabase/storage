import { parseCommaSeparatedList } from '@internal/queue/overflow'

type JobsAction = 'backup' | 'list' | 'restore'
type JobsGroupBy = 'summary' | 'tenant'
type JobsSource = 'backup' | 'job'

interface JobsClientOptions {
  confirmAll?: boolean
  eventTypes?: string[]
  groupBy: JobsGroupBy
  limit?: number
  maxPending: number
  queueName?: string
  sleepMs: number
  source: JobsSource
  tenantRefs?: string[]
}

interface JobsClientConfig extends JobsClientOptions {
  adminApiKey: string
  adminUrl: string
}

interface JobsClientDependencies {
  fetch?: typeof fetch
  sleep?: (ms: number) => Promise<void>
}

const JOBS_MAX_PENDING_DEFAULT = 50_000
const JOBS_SLEEP_MS_DEFAULT = 1_000
const JOBS_BACKOFF_MAX_MS = 60_000
const JOBS_CLIENT_REQUEST_FAILURE_MESSAGE = 'Jobs client request failed'
const JOBS_BACKUP_SCOPE_MESSAGE =
  'Backup requires JOBS_QUEUE_NAME, JOBS_EVENT_TYPES, or JOBS_TENANT_REFS unless JOBS_BACKUP_CONFIRM_ALL=true'

class UnscopedBackupError extends Error {}
class JobsClientResponseError extends Error {}

function parseOptionalBoolean(value: string | undefined, errorMessage: string) {
  const normalized = value?.trim().toLowerCase()
  if (!normalized) {
    return undefined
  }
  if (normalized === 'true' || normalized === 'false') {
    return normalized === 'true'
  }
  throw new Error(errorMessage)
}

function parsePositiveInteger(value: string, errorMessage: string) {
  const normalized = value.trim()
  const parsed = Number(normalized)
  if (!/^\d+$/.test(normalized) || !Number.isSafeInteger(parsed) || parsed === 0) {
    throw new Error(errorMessage)
  }
  return parsed
}

export function resolveJobsAdminUrl(
  baseUrl: string,
  requestPath: string,
  query?: Record<string, string | undefined>
) {
  const url = new URL(baseUrl)
  url.pathname = `${url.pathname.replace(/\/+$/, '')}/${requestPath.replace(/^\/+/, '')}`

  for (const [key, value] of Object.entries(query ?? {})) {
    if (value !== undefined) {
      url.searchParams.set(key, value)
    }
  }

  return url
}

export function parseJobsOptions(env: NodeJS.ProcessEnv): JobsClientOptions | string {
  const source = (env.JOBS_SOURCE?.trim() || 'job') as JobsSource
  const groupBy = (env.JOBS_GROUP_BY?.trim() || 'summary') as JobsGroupBy

  if (source !== 'job' && source !== 'backup') {
    return 'JOBS_SOURCE must be either job or backup'
  }

  if (groupBy !== 'summary' && groupBy !== 'tenant') {
    return 'JOBS_GROUP_BY must be either summary or tenant'
  }

  try {
    return {
      confirmAll: parseOptionalBoolean(
        env.JOBS_BACKUP_CONFIRM_ALL,
        'JOBS_BACKUP_CONFIRM_ALL must be either true or false'
      ),
      queueName: env.JOBS_QUEUE_NAME?.trim() || undefined,
      eventTypes: parseCommaSeparatedList(env.JOBS_EVENT_TYPES),
      tenantRefs: parseCommaSeparatedList(env.JOBS_TENANT_REFS),
      source,
      groupBy,
      limit: env.JOBS_LIMIT
        ? parsePositiveInteger(env.JOBS_LIMIT, 'JOBS_LIMIT must be a positive integer')
        : undefined,
      maxPending: env.JOBS_MAX_PENDING
        ? parsePositiveInteger(env.JOBS_MAX_PENDING, 'JOBS_MAX_PENDING must be a positive integer')
        : JOBS_MAX_PENDING_DEFAULT,
      sleepMs: env.JOBS_SLEEP_MS
        ? parsePositiveInteger(env.JOBS_SLEEP_MS, 'JOBS_SLEEP_MS must be a positive integer')
        : JOBS_SLEEP_MS_DEFAULT,
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
      redirect: 'error' as const,
    }
  }

  if (
    action === 'backup' &&
    !config.confirmAll &&
    !config.queueName &&
    !config.eventTypes?.length &&
    !config.tenantRefs?.length
  ) {
    throw new UnscopedBackupError(JOBS_BACKUP_SCOPE_MESSAGE)
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
      ...(action === 'backup' && config.confirmAll ? { confirmAll: true } : {}),
    }),
    redirect: 'error' as const,
  }
}

export function buildJobsCountRequest(config: JobsClientConfig) {
  return {
    method: 'GET',
    url: resolveJobsAdminUrl(config.adminUrl, '/queue/overflow/count'),
    headers: new Headers({
      ApiKey: config.adminApiKey,
    }),
    redirect: 'error' as const,
  }
}

async function assertJsonResponse(response: Response) {
  if (response.ok) {
    return response.json()
  }

  const body: unknown = await response.json().catch(() => undefined)
  const message =
    typeof body === 'object' &&
    body !== null &&
    'message' in body &&
    typeof body.message === 'string'
      ? body.message
      : undefined
  const detail = message ? `: ${message}` : ''

  throw new JobsClientResponseError(
    `${JOBS_CLIENT_REQUEST_FAILURE_MESSAGE} (${response.status})${detail}`
  )
}

async function fetchJobsJson(
  fetchRequest: typeof fetch,
  request: ReturnType<typeof buildJobsRequest> | ReturnType<typeof buildJobsCountRequest>
) {
  const { url, ...init } = request
  return assertJsonResponse(await fetchRequest(url, init))
}

function fail(message: string) {
  process.exitCode = 1
  console.error(message)
  return false
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })
}

function formatWait(ms: number) {
  return ms % 1_000 === 0 ? `${ms / 1_000}s` : `${ms}ms`
}

export async function main(
  env: NodeJS.ProcessEnv = process.env,
  argv: string[] = process.argv,
  dependencies: JobsClientDependencies = {}
): Promise<boolean> {
  const action = argv[2]

  if (action !== 'list' && action !== 'backup' && action !== 'restore') {
    return fail('Please provide an action: list, backup, or restore')
  }

  const adminUrl = env.ADMIN_URL
  if (!adminUrl) {
    return fail('Please provide ADMIN_URL')
  }

  const adminApiKey = env.ADMIN_API_KEY
  if (!adminApiKey) {
    return fail('Please provide ADMIN_API_KEY')
  }

  const options = parseJobsOptions(env)
  if (typeof options === 'string') {
    return fail(options)
  }

  const config: JobsClientConfig = { ...options, adminApiKey, adminUrl }

  try {
    const fetchRequest = dependencies.fetch ?? globalThis.fetch

    if (action === 'restore') {
      const wait = dependencies.sleep ?? sleep
      const restoreRequest = buildJobsRequest(action, config)
      const backlogRequest = buildJobsCountRequest(config)
      let backoffMs = Math.min(config.sleepMs, JOBS_BACKOFF_MAX_MS)
      let batches = 0
      let conflictCount = 0
      let movedCount = 0

      while (true) {
        while (true) {
          const backlog = (await fetchJobsJson(fetchRequest, backlogRequest)) as {
            totalCount: number
          }

          if (backlog.totalCount <= config.maxPending) {
            break
          }

          console.error(
            `Queue backlog ${backlog.totalCount} above ${config.maxPending}, waiting ${formatWait(backoffMs)}`
          )
          await wait(backoffMs)
          backoffMs = Math.min(backoffMs * 2, JOBS_BACKOFF_MAX_MS)
        }

        const data = (await fetchJobsJson(fetchRequest, restoreRequest)) as {
          conflictCount: number
          hasMore: boolean
          movedCount: number
        }

        batches += 1
        conflictCount += data.conflictCount
        movedCount += data.movedCount
        backoffMs = Math.min(config.sleepMs, JOBS_BACKOFF_MAX_MS)
        console.error(
          `Restore batch ${batches}: moved ${data.movedCount}, conflicts ${data.conflictCount}`
        )

        if (data.hasMore && data.movedCount + data.conflictCount === 0) {
          return fail('Restore reported hasMore=true without moving or dropping any rows')
        }

        if (!data.hasMore) {
          break
        }

        await wait(config.sleepMs)
      }

      console.log(JSON.stringify({ batches, conflictCount, movedCount }, null, 2))
      return true
    }

    const request = buildJobsRequest(action, config)
    const data = await fetchJobsJson(fetchRequest, request)
    console.log(JSON.stringify(data, null, 2))
    return true
  } catch (error) {
    if (error instanceof UnscopedBackupError) {
      return fail(JOBS_BACKUP_SCOPE_MESSAGE)
    }

    if (error instanceof JobsClientResponseError) {
      return fail(error.message)
    }

    return fail(JOBS_CLIENT_REQUEST_FAILURE_MESSAGE)
  }
}

if (require.main === module) {
  main(process.env).catch(() => {
    process.exitCode = 1
    console.error(JOBS_CLIENT_REQUEST_FAILURE_MESSAGE)
  })
}
