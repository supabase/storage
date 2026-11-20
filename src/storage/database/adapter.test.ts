import { describe, expect, it } from 'vitest'
import { wasNoncurrentVersion } from './adapter'

describe('wasNoncurrentVersion', () => {
  it('is true for a row archived before it was removed', () => {
    expect(wasNoncurrentVersion({ archived_at: '2026-01-01T00:00:00Z' })).toBe(true)
  })

  it('is false for a row that was current (archived_at null)', () => {
    expect(wasNoncurrentVersion({ archived_at: null })).toBe(false)
  })

  it('is false when archived_at was not selected on the row', () => {
    expect(wasNoncurrentVersion({})).toBe(false)
  })
})
