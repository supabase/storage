import { S3VectorsClient } from '@aws-sdk/client-s3vectors'
import { createAgent } from '@internal/http'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const config = vi.hoisted(() => ({
  storageS3Region: undefined as string | undefined,
  vectorBucketRegion: undefined as string | undefined,
}))

const agent = vi.hoisted(() => {
  const dispatcher = { request: vi.fn(), close: vi.fn(), destroy: vi.fn() }
  return {
    dispatcher,
    close: vi.fn<() => Promise<void>>(async () => undefined),
  }
})

vi.mock('@internal/http', () => ({ createAgent: vi.fn(() => agent) }))

vi.mock('../../../../config', () => ({
  getConfig: () => config,
}))

vi.mock('@aws-sdk/client-s3vectors', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@aws-sdk/client-s3vectors')>()),
  S3VectorsClient: vi.fn(function () {
    return { destroy: vi.fn() }
  }),
}))

describe('createS3VectorClient', () => {
  beforeEach(() => {
    vi.resetModules()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it.each([
    {
      name: 'prefers the vector bucket region',
      configured: { storageS3Region: 'us-east-1', vectorBucketRegion: 'eu-west-1' },
      expectedRegion: 'eu-west-1',
    },
    {
      name: 'falls back to the storage S3 region',
      configured: { storageS3Region: 'sa-east-1', vectorBucketRegion: undefined },
      expectedRegion: 'sa-east-1',
    },
  ])('$name', async ({ configured, expectedRegion }) => {
    Object.assign(config, configured)
    const { createS3VectorClient } = await import('./s3-vector')
    const client = createS3VectorClient()
    const Client = vi.mocked(S3VectorsClient)

    expect(Client).toHaveBeenCalledOnce()
    expect(Client).toHaveBeenCalledWith({
      region: expectedRegion,
      requestHandler: expect.any(UndiciHttpHandler),
    })
    expect(createAgent).toHaveBeenCalledExactlyOnceWith('s3_vector', {
      maxSockets: 50,
      connectionTimeoutMs: 0,
      bodyTimeoutMs: 0,
    })
    const handler = Client.mock.calls[0][0]!.requestHandler as UndiciHttpHandler
    expect(handler.httpHandlerConfigs().dispatcher).toBe(agent.dispatcher)
    expect(client).toBe(Client.mock.results[0].value)
    await client.close()
  })

  it('waits for requests to drain before destroying the SDK client, once', async () => {
    const { createS3VectorClient } = await import('./s3-vector')
    const drained = Promise.withResolvers<void>()
    agent.close.mockReturnValueOnce(drained.promise)
    const client = createS3VectorClient()
    const closing = client.close()

    expect(client.close()).toBe(closing)
    expect(agent.close).toHaveBeenCalledOnce()
    expect(client.destroy).not.toHaveBeenCalled()
    drained.resolve()
    await closing
    expect(client.destroy).toHaveBeenCalledOnce()
  })

  it('destroys the SDK client when requests do not drain within two seconds', async () => {
    vi.useFakeTimers()
    const { createS3VectorClient } = await import('./s3-vector')
    agent.close.mockReturnValueOnce(new Promise<void>(() => {}))
    const client = createS3VectorClient()
    const closing = client.close()
    let settled = false
    void closing.then(() => {
      settled = true
    })

    await vi.advanceTimersByTimeAsync(1999)
    expect(client.destroy).not.toHaveBeenCalled()
    expect(settled).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    await closing
    expect(client.destroy).toHaveBeenCalledOnce()
  })
})
