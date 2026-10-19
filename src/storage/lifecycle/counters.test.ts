import { logSchema } from '@internal/monitoring'
import { describe, expect, it, vi } from 'vitest'
import { lifecycleDeletionCounters } from './counters'

const context = { tenantId: 'tenant', bucketId: 'bucket' }

describe('lifecycle deletion counters', () => {
  it.each([
    null,
    {},
    { size: null },
    { size: -1 },
    { size: 1.5 },
    { size: 'invalid' },
    { size: '' },
    { size: ' ' },
    { size: true },
    { size: Number.MAX_SAFE_INTEGER + 1 },
    { size: Number.NaN },
    { size: Number.POSITIVE_INFINITY },
  ])('counts invalid metadata %j as zero bytes without losing the object count', (metadata) => {
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    expect(
      lifecycleDeletionCounters(
        [
          { is_delete_marker: false, metadata },
          { is_delete_marker: false, metadata: { size: 10 } },
        ],
        context
      )
    ).toEqual({ objectVersions: 2, deleteMarkers: 0, bytes: 10 })
    expect(warning).toHaveBeenCalledExactlyOnceWith(
      expect.anything(),
      expect.stringContaining('invalid object sizes'),
      expect.objectContaining({
        tenantId: context.tenantId,
        metadata: JSON.stringify({ bucketId: context.bucketId, invalidSizeCount: 1 }),
      })
    )
  })

  it.each([0, 1, '1', Number.MAX_SAFE_INTEGER])('accepts size %j', (size) => {
    const warning = vi.spyOn(logSchema, 'warning')
    expect(
      lifecycleDeletionCounters([{ is_delete_marker: false, metadata: { size } }], context)
    ).toEqual({ objectVersions: 1, deleteMarkers: 0, bytes: Number(size) })
    expect(warning).not.toHaveBeenCalled()
  })

  it('counts delete markers separately without inspecting their size', () => {
    const warning = vi.spyOn(logSchema, 'warning')
    expect(
      lifecycleDeletionCounters(
        [
          { is_delete_marker: true, metadata: null },
          { is_delete_marker: true, metadata: { size: 100 } },
        ],
        context
      )
    ).toEqual({ objectVersions: 0, deleteMarkers: 2, bytes: 0 })
    expect(warning).not.toHaveBeenCalled()
  })

  it('counts a row that would overflow the safe integer range as invalid', () => {
    const warning = vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    expect(
      lifecycleDeletionCounters(
        [
          { is_delete_marker: false, metadata: { size: Number.MAX_SAFE_INTEGER } },
          { is_delete_marker: false, metadata: { size: 1 } },
        ],
        context
      )
    ).toEqual({ objectVersions: 2, deleteMarkers: 0, bytes: Number.MAX_SAFE_INTEGER })
    expect(warning).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      expect.objectContaining({ metadata: expect.stringContaining('"invalidSizeCount":1') })
    )
  })
})
