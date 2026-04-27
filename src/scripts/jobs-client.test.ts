import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  buildJobsRequest,
  main,
  parseJobsConfig,
  parseJobsCsv,
  resolveJobsAdminUrl,
} from './jobs-client'

describe('parseJobsCsv', () => {
  it('returns undefined for empty input', () => {
    expect(parseJobsCsv(undefined)).toBeUndefined()
    expect(parseJobsCsv(' , , ')).toBeUndefined()
  })

  it('trims and de-duplicates values', () => {
    expect(parseJobsCsv('tenant-a, tenant-b,tenant-a')).toEqual(['tenant-a', 'tenant-b'])
  })
})

describe('resolveJobsAdminUrl', () => {
  it('preserves base URL path prefixes and query params', () => {
    const url = resolveJobsAdminUrl('http://localhost:54321/admin/', '/queue/overflow', {
      source: 'backup',
      groupBy: 'tenant',
    })

    expect(url.toString()).toBe(
      'http://localhost:54321/admin/queue/overflow?source=backup&groupBy=tenant'
    )
  })
})

describe('parseJobsConfig', () => {
  it('uses defaults when optional env is omitted', () => {
    expect(
      parseJobsConfig({
        ADMIN_URL: 'http://localhost:54321/admin',
        ADMIN_API_KEY: 'test-key',
      })
    ).toEqual({
      adminApiKey: 'test-key',
      adminUrl: 'http://localhost:54321/admin',
      queueName: undefined,
      eventTypes: undefined,
      tenantRefs: undefined,
      source: 'job',
      groupBy: 'summary',
      limit: undefined,
    })
  })

  it('rejects invalid JOBS_SOURCE and JOBS_LIMIT values', () => {
    expect(
      parseJobsConfig({
        ADMIN_URL: 'http://localhost:54321/admin',
        ADMIN_API_KEY: 'test-key',
        JOBS_SOURCE: 'archive',
      })
    ).toBe('JOBS_SOURCE must be either job or backup')

    expect(
      parseJobsConfig({
        ADMIN_URL: 'http://localhost:54321/admin',
        ADMIN_API_KEY: 'test-key',
        JOBS_LIMIT: '0',
      })
    ).toBe('JOBS_LIMIT must be a positive integer')
  })
})

describe('buildJobsRequest', () => {
  const baseConfig = {
    adminApiKey: 'test-key',
    adminUrl: 'http://localhost:54321/admin',
    queueName: 'webhooks',
    eventTypes: ['ObjectRemoved:Delete'],
    tenantRefs: ['tenant-a'],
    source: 'backup' as const,
    groupBy: 'tenant' as const,
    limit: 123,
  }

  it('builds list requests with query params', () => {
    const request = buildJobsRequest('list', baseConfig)

    expect(request.method).toBe('GET')
    expect(request.body).toBeUndefined()
    expect(request.url.toString()).toBe(
      'http://localhost:54321/admin/queue/overflow?source=backup&groupBy=tenant&name=webhooks&eventTypes=ObjectRemoved%3ADelete&tenantRefs=tenant-a&limit=123'
    )
    expect((request.headers as Headers).get('ApiKey')).toBe('test-key')
  })

  it('builds backup requests with a JSON body', () => {
    const request = buildJobsRequest('backup', baseConfig)

    expect(request.method).toBe('POST')
    expect(request.url.toString()).toBe('http://localhost:54321/admin/queue/overflow/backup')
    expect(JSON.parse(request.body as string)).toEqual({
      name: 'webhooks',
      eventTypes: ['ObjectRemoved:Delete'],
      tenantRefs: ['tenant-a'],
      limit: 123,
    })
    expect((request.headers as Headers).get('ApiKey')).toBe('test-key')
    expect((request.headers as Headers).get('Content-Type')).toBe('application/json')
  })
})

describe('main', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    process.exitCode = undefined
  })

  it('sets a non-zero exit code for invalid actions', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})

    await expect(main({}, ['node', 'jobs-client.ts', 'invalid'])).resolves.toBe(false)

    expect(process.exitCode).toBe(1)
    expect(console.error).toHaveBeenCalledWith('Please provide an action: list, backup, or restore')
  })

  it('prints the JSON response for successful requests', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ movedCount: 3 }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
        },
      })
    )
    vi.spyOn(console, 'log').mockImplementation(() => {})

    await expect(
      main(
        {
          ADMIN_URL: 'http://localhost:54321/admin',
          ADMIN_API_KEY: 'test-key',
          JOBS_QUEUE_NAME: 'webhooks',
        },
        ['node', 'jobs-client.ts', 'restore']
      )
    ).resolves.toBe(true)

    expect(console.log).toHaveBeenCalledWith('{\n  "movedCount": 3\n}')
  })
})
