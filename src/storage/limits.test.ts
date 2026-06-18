import { afterEach, describe, expect, it, vi } from 'vitest'

const ENV = { ...process.env }

describe('enforceDeleteObjectsLimit', () => {
  afterEach(() => {
    process.env = { ...ENV }
    vi.doUnmock('../internal/database/tenant')
    vi.resetModules()
  })

  it('does not enforce the object request cap until hard limits are enabled', async () => {
    process.env.MULTI_TENANT = 'false'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'false'
    vi.resetModules()

    const { enforceDeleteObjectsLimit, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    await expect(
      enforceDeleteObjectsLimit('tenant-id', MAX_OBJECTS_PER_REQUEST + 1)
    ).resolves.toBeUndefined()
  })

  it('enforces the default object request cap when hard limits are enabled', async () => {
    process.env.MULTI_TENANT = 'false'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    vi.resetModules()

    const { enforceDeleteObjectsLimit, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    await expect(
      enforceDeleteObjectsLimit('tenant-id', MAX_OBJECTS_PER_REQUEST + 1)
    ).rejects.toMatchObject({
      code: 'InvalidRequest',
      message: `Bulk object requests are limited to ${MAX_OBJECTS_PER_REQUEST} objects per request.`,
    })
  })

  it('uses the tenant delete objects limit in multitenant mode', async () => {
    process.env.MULTI_TENANT = 'true'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    const getDeleteObjectsLimit = vi.fn().mockResolvedValue(2000)
    vi.doMock('../internal/database/tenant', () => ({
      getDeleteObjectsLimit,
      getFeatures: vi.fn(),
      getFileSizeLimit: vi.fn(),
    }))
    vi.resetModules()

    const { enforceDeleteObjectsLimit } = await import('./limits')

    await expect(enforceDeleteObjectsLimit('tenant-id', 1500)).resolves.toBeUndefined()
    await expect(enforceDeleteObjectsLimit('tenant-id', 2001)).rejects.toMatchObject({
      code: 'InvalidRequest',
      message: 'Bulk object requests are limited to 2000 objects per request.',
    })
    expect(getDeleteObjectsLimit).toHaveBeenCalledWith('tenant-id')
  })
})
