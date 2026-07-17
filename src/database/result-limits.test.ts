import { describe, expect, it } from 'vitest'
import { readConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import { enforceResultLimits } from './result-limits.js'

describe('database result limits', () => {
  it('returns bounded results unchanged', () => {
    const config = readConfig({
      DATABASE_WATT_MAX_RESULT_ROWS: '2',
      DATABASE_WATT_MAX_RESULT_BYTES: '1000',
    })
    const result = { rowCount: 1, rows: [{ id: 1 }] }

    expect(enforceResultLimits(result, config)).toBe(result)
  })

  it('rejects row limits without returning partial rows', () => {
    const config = readConfig({
      DATABASE_WATT_MAX_RESULT_ROWS: '1',
      DATABASE_WATT_MAX_RESULT_BYTES: '1000',
    })

    expect(() =>
      enforceResultLimits({ rowCount: 2, rows: [{ id: 1 }, { id: 2 }] }, config)
    ).toThrow(DatabaseWattError)
  })

  it('rejects byte limits', () => {
    const config = readConfig({
      DATABASE_WATT_MAX_RESULT_ROWS: '10',
      DATABASE_WATT_MAX_RESULT_BYTES: '20',
    })

    expect(() =>
      enforceResultLimits({ rowCount: 1, rows: [{ value: 'x'.repeat(100) }] }, config)
    ).toThrow(/byte limit/)
  })
})
