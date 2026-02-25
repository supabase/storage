import { isValidKey } from '../storage/limits'

describe('isValidKey', () => {
  test('accepts unicode object names', () => {
    expect(isValidKey('folder/일이삼/🙂/a b.txt')).toBe(true)
  })

  test('accepts tab, newline, and carriage return', () => {
    expect(isValidKey('a\tb\nc\rd')).toBe(true)
  })

  test('rejects empty keys', () => {
    expect(isValidKey('')).toBe(false)
  })

  test('rejects ASCII control characters except tab/newline/carriage return', () => {
    expect(isValidKey('invalid\x01name')).toBe(false)
  })

  test('rejects non-characters U+FFFE and U+FFFF', () => {
    expect(isValidKey(`invalid${'\uFFFE'}`)).toBe(false)
    expect(isValidKey(`invalid${'\uFFFF'}`)).toBe(false)
  })

  test('rejects lone surrogate code units', () => {
    expect(isValidKey(`bad${'\uD83D'}`)).toBe(false)
    expect(isValidKey(`bad${'\uDC00'}`)).toBe(false)
  })

  test('accepts valid surrogate pairs', () => {
    expect(isValidKey('ok🙂name')).toBe(true)
  })
})
