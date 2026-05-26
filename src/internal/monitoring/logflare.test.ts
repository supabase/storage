import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { defaultPreparePayloadMock } = vi.hoisted(() => ({
  defaultPreparePayloadMock: vi.fn(),
}))

vi.mock('pino-logflare', () => ({
  defaultPreparePayload: defaultPreparePayloadMock,
}))

type PayloadMeta = Parameters<typeof defaultPreparePayloadMock>[1]

describe('logflare helpers', () => {
  beforeEach(() => {
    defaultPreparePayloadMock.mockReset()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.resetModules()
  })

  it('promotes project and sbReqId (as request_id) onto the prepared payload', async () => {
    defaultPreparePayloadMock.mockReturnValue({
      log_entry: 'hello',
      metadata: { level: 'info' },
    })

    const { onPreparePayload } = await import('./logflare')
    const meta = {} as PayloadMeta
    const payload = {
      project: 'tenant-a',
      sbReqId: 'sb-req-123',
      msg: { text: 'hello' },
    }

    expect(onPreparePayload(payload, meta)).toEqual({
      log_entry: 'hello',
      metadata: { level: 'info' },
      project: 'tenant-a',
      request_id: 'sb-req-123',
    })
    expect(defaultPreparePayloadMock).toHaveBeenCalledWith(payload, meta)
  })

  it('leaves project and request_id undefined when they are missing', async () => {
    defaultPreparePayloadMock.mockReturnValue({
      log_entry: 'hello',
      metadata: {},
    })

    const { onPreparePayload } = await import('./logflare')
    const payload = {
      msg: { text: 'hello' },
    }

    expect(onPreparePayload(payload, {} as PayloadMeta)).toEqual({
      log_entry: 'hello',
      metadata: {},
      project: undefined,
      request_id: undefined,
    })
  })

  it('logs transport errors to console.error', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const { onError } = await import('./logflare')
    const err = new Error('boom')
    err.stack = 'stack-trace'

    onError({}, err)

    expect(errorSpy).toHaveBeenCalledWith('[Logflare][Error] boom - stack-trace')
  })
})
