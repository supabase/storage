import type { JobContext } from '@supabase-labs/wave-core'
import { vi } from 'vitest'
import type { WirePayload } from '@internal/queue'
import type { JwksRollUrlSigningKeyPayload } from './jwks-roll-url-signing-key'

const { mockRollUrlSigningJwk, mockInfo, mockError } = vi.hoisted(() => ({
  mockRollUrlSigningJwk: vi.fn(),
  mockInfo: vi.fn(),
  mockError: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  jwksManager: {
    rollUrlSigningJwk: mockRollUrlSigningJwk,
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: mockError,
    warning: vi.fn(),
  },
}))

// Minimal stand-in for `storageEvent`: enough class surface for TopicHandler, without
// pulling base.ts's storage/database import graph into the unit test.
vi.mock('../base', () => ({
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))

vi.mock('../topics', () => ({
  TOPICS: { jwksRollUrlSigningKey: 'tenants-jwks-roll-url-signing-key-v1' },
  systemRetry: (topic: string) => ({
    maxAttempts: 4,
    backoffMs: 5_000,
    deadLetter: `${topic}-dead-letter`,
  }),
}))

import { JwksRollUrlSigningKeyHandler } from './jwks-roll-url-signing-key'

function makeCtx(
  data: WirePayload<JwksRollUrlSigningKeyPayload>,
  attempt = 1
): JobContext<WirePayload<JwksRollUrlSigningKeyPayload>> {
  return {
    topic: 'tenants-jwks-roll-url-signing-key-v1',
    group: 'tenants-jwks-roll-url-signing-key-v1',
    message: { id: 'job-1', data, headers: {}, timestamp: 0, attempt },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

const payload: WirePayload<JwksRollUrlSigningKeyPayload> = {
  tenantId: 'tenant-a',
  tenant: {
    ref: 'tenant-a',
    host: '',
  },
  sbReqId: 'sb-req-123',
  region: 'local',
}

describe('JwksRollUrlSigningKeyHandler.handle', () => {
  const handler = new JwksRollUrlSigningKeyHandler()

  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('logs sbReqId on success', async () => {
    mockRollUrlSigningJwk.mockResolvedValue({
      oldKid: 'old-kid',
      newKid: 'new-kid',
    })

    await expect(handler.handle(makeCtx(payload))).resolves.toBeUndefined()

    expect(mockRollUrlSigningJwk).toHaveBeenCalledWith('tenant-a')
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Jwks] rolled url signing key for tenant tenant-a (old: old-kid, new: new-kid)',
      expect.objectContaining({
        type: 'jwks',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
  })

  it('logs sbReqId on failure', async () => {
    const error = new Error('boom')
    mockRollUrlSigningJwk.mockRejectedValue(error)

    await expect(handler.handle(makeCtx(payload))).rejects.toThrow(error)

    expect(mockError).toHaveBeenCalledWith(
      expect.anything(),
      '[Jwks] roll url signing key failed for tenant tenant-a',
      expect.objectContaining({
        type: 'jwks',
        error,
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
  })
})
