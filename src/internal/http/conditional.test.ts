import { matchesETag, parseHttpDate, toSeconds, unquoteETag } from './conditional'

describe('conditional HTTP helpers', () => {
  describe('parseHttpDate', () => {
    it.each([
      ['Sun, 06 Nov 1994 08:49:37 GMT', '1994-11-06T08:49:37.000Z'],
      ['Sat, 31 Dec 2016 23:59:60 GMT', '2016-12-31T23:59:59.000Z'],
      ['Sunday, 06-Nov-94 08:49:37 GMT', '1994-11-06T08:49:37.000Z'],
      ['Sunday, 06-Nov-26 08:49:37 GMT', '2026-11-06T08:49:37.000Z'],
      ['Wednesday, 01-Jan-70 00:00:00 GMT', '2070-01-01T00:00:00.000Z'],
      ['Monday, 01-Jun-76 00:00:00 GMT', '2076-06-01T00:00:00.000Z'],
      ['Monday, 01-Jun-76 00:00:01 GMT', '1976-06-01T00:00:01.000Z'],
      ['Friday, 31-Dec-76 00:00:00 GMT', '1976-12-31T00:00:00.000Z'],
      ['Sun Nov  6 08:49:37 1994', '1994-11-06T08:49:37.000Z'],
      ['Sun Nov 16 08:49:37 1994', '1994-11-16T08:49:37.000Z'],
    ])('parses RFC 9110 HTTP-date %j', (input, expectedIso) => {
      expect(parseHttpDate(input, new Date('2026-06-01T00:00:00.000Z'))?.toISOString()).toBe(
        expectedIso
      )
    })

    it.each([
      '0',
      '12345',
      '2022-10-01T00:00:00.000Z',
      'not-a-date',
      '',
      'Sun, 31 Feb 1994 08:49:37 GMT',
      'Sun, 06 Nov 1994 25:00:00 GMT',
      'Sun, 06 Nov 1994 08:49:61 GMT',
      'Sun, 06 Nov 1994 08:49:37 UTC',
    ])('rejects invalid HTTP-date %j', (input) => {
      expect(parseHttpDate(input)).toBeUndefined()
    })
  })

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
