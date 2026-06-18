import { afterEach, describe, expect, it, vi } from 'vitest'

describe('objectRequestLimitSchema', () => {
  afterEach(() => {
    delete process.env.REQUEST_HARD_LIMITS_ENABLED
    vi.resetModules()
  })

  it('omits maxItems when hard limits are disabled', async () => {
    delete process.env.REQUEST_HARD_LIMITS_ENABLED
    vi.resetModules()

    const { objectRequestLimitSchema, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    expect(objectRequestLimitSchema()).toEqual({
      description: `At most ${MAX_OBJECTS_PER_REQUEST} objects can be deleted per request.`,
    })
  })

  it('caps maxItems at the object request limit when hard limits are enabled', async () => {
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    vi.resetModules()

    const { objectRequestLimitSchema, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    expect(objectRequestLimitSchema()).toEqual({
      maxItems: MAX_OBJECTS_PER_REQUEST,
      description: `At most ${MAX_OBJECTS_PER_REQUEST} objects can be deleted per request.`,
    })
  })
})
