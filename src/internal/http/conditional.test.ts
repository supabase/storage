import { matchesETag, toSeconds, unquoteETag } from './conditional'

describe('conditional HTTP helpers', () => {
  describe('toSeconds', () => {
    it('truncates milliseconds to whole seconds', () => {
      expect(toSeconds(new Date('2026-01-01T00:00:01.999Z'))).toBe(
        Math.floor(Date.parse('2026-01-01T00:00:01.000Z') / 1000)
      )
    })

    it('returns NaN for an invalid date', () => {
      expect(toSeconds(new Date('not-a-date'))).toBeNaN()
    })
  })

  describe('unquoteETag', () => {
    it.each([
      ['"abc"', 'abc'],
      ['W/"abc"', 'abc'],
      ['  W/"abc"  ', 'abc'],
      ['abc', 'abc'],
      ['""', ''],
      ['W/""', ''],
      ['w/"abc"', 'w/"abc"'],
      ['W/ "abc"', ' "abc"'],
    ])('normalizes %j to %j', (input, expected) => {
      expect(unquoteETag(input)).toBe(expected)
    })
  })

  describe('matchesETag', () => {
    it.each([
      ['"abc"', '"abc"'],
      ['W/"abc"', '"abc"'],
      ['"abc"', 'W/"abc"'],
      ['W/"abc"', 'W/"abc"'],
      ['abc', 'W/"abc"'],
      ['"old", W/"abc", "other"', 'W/"abc"'],
      ['"a,b"', 'W/"a,b"'],
      ['"x,*,y", "a,b"', '"a,b"'],
      ['*', 'W/"abc"'],
      [' * ', '"abc"'],
      ['""', '""'],
      ['"old", ""', '""'],
    ])('matches %j against %j', (condition, eTag) => {
      expect(matchesETag(condition, eTag)).toBe(true)
    })

    it.each([
      ['"other"', '"abc"'],
      ['w/"abc"', '"abc"'],
      ['W/ "abc"', '"abc"'],
      ['"stale,*,etag"', '"abc"'],
      ['', '"abc"'],
      ['   ', '""'],
      ['"foo", ', '""'],
      [', ,', '""'],
      ['', '""'],
    ])('does not match %j against %j', (condition, eTag) => {
      expect(matchesETag(condition, eTag)).toBe(false)
    })
    it('can disable wildcard matching', () => {
      expect(matchesETag('*', 'W/"abc"', { allowWildcard: false })).toBe(false)
      expect(matchesETag('*, W/"abc"', 'W/"abc"', { allowWildcard: false })).toBe(true)
    })
  })
})
