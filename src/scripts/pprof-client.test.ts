import {
  parseBooleanEnv,
  parseBooleanEnvWithDefault,
  parseNonNegativeIntegerEnv,
  parsePositiveIntegerEnv,
  parsePprofTarget,
} from './pprof-client'

describe('parsePprofTarget', () => {
  it('uses the default seconds when only the type is provided', () => {
    expect(parsePprofTarget('profile', 60)).toEqual({
      seconds: 60,
      type: 'profile',
    })
    expect(parsePprofTarget('heap', 30)).toEqual({
      seconds: 30,
      type: 'heap',
    })
  })

  it('parses inline seconds overrides', () => {
    expect(parsePprofTarget('profile:10', 60)).toEqual({
      seconds: 10,
      type: 'profile',
    })
    expect(parsePprofTarget('heap:5', 60)).toEqual({
      seconds: 5,
      type: 'heap',
    })
  })

  it('rejects invalid targets', () => {
    expect(() => parsePprofTarget(undefined, 60)).toThrow('Usage:')
    expect(() => parsePprofTarget('cpu', 60)).toThrow('Usage:')
    expect(() => parsePprofTarget('profile:abc', 60)).toThrow('seconds must be a positive integer')
    expect(() => parsePprofTarget('heap:0', 60)).toThrow('seconds must be a positive integer')
    expect(() => parsePprofTarget('profile:10abc', 60)).toThrow(
      'seconds must be a positive integer'
    )
    expect(() => parsePprofTarget('profile:10:extra', 60)).toThrow('Usage:')
  })
})

describe('pprof client env parsing', () => {
  it('parses boolean env values', () => {
    expect(parseBooleanEnv(undefined)).toBeUndefined()
    expect(parseBooleanEnv('true')).toBe(true)
    expect(parseBooleanEnv('OFF')).toBe(false)
    expect(() => parseBooleanEnv('maybe')).toThrow('Invalid boolean value')
  })

  it('parses boolean env values with defaults', () => {
    expect(parseBooleanEnvWithDefault(undefined, true)).toBe(true)
    expect(parseBooleanEnvWithDefault(undefined, false)).toBe(false)
    expect(parseBooleanEnvWithDefault('no', true)).toBe(false)
  })

  it('parses positive integer env values strictly', () => {
    expect(parsePositiveIntegerEnv(undefined, 'PPROF_SECONDS', 60)).toBe(60)
    expect(parsePositiveIntegerEnv('90', 'PPROF_SECONDS', 60)).toBe(90)
    expect(() => parsePositiveIntegerEnv('0', 'PPROF_SECONDS', 60)).toThrow(
      'PPROF_SECONDS must be a positive integer'
    )
    expect(() => parsePositiveIntegerEnv('12ms', 'PPROF_SECONDS', 60)).toThrow(
      'PPROF_SECONDS must be a positive integer'
    )
  })

  it('parses non-negative integer env values strictly', () => {
    expect(parseNonNegativeIntegerEnv(undefined, 'PPROF_WORKER_ID')).toBeUndefined()
    expect(parseNonNegativeIntegerEnv('0', 'PPROF_WORKER_ID')).toBe(0)
    expect(parseNonNegativeIntegerEnv('7', 'PPROF_WORKER_ID')).toBe(7)
    expect(() => parseNonNegativeIntegerEnv('-1', 'PPROF_WORKER_ID')).toThrow(
      'PPROF_WORKER_ID must be a non-negative integer'
    )
    expect(() => parseNonNegativeIntegerEnv('7x', 'PPROF_WORKER_ID')).toThrow(
      'PPROF_WORKER_ID must be a non-negative integer'
    )
  })
})
