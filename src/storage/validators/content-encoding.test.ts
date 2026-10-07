import { ErrorCode } from '@internal/errors'
import { validateContentEncoding } from './content-encoding'

describe('Content-Encoding', () => {
  it('preserves valid coding lists', () => {
    expect(validateContentEncoding(' gzip, br ')).toBe('gzip, br')
    expect(validateContentEncoding(undefined)).toBeUndefined()
  })

  it.each([
    ['gzip, ', 'gzip'],
    [', gzip,,br,', 'gzip, br'],
    ['', undefined],
    [' , \t, ', undefined],
    [', aws-chunked,,identity,', undefined],
  ])('ignores empty elements in %j', (value, expected) => {
    expect(validateContentEncoding(value)).toBe(expected)
  })

  it.each([
    'gzip\r\nX-Foo: bar',
    'gzip;',
    'gzip br',
    'gzip,\u00a0br',
    123,
    null,
    'g'.repeat(8193),
  ])('rejects invalid coding %s', (value) => {
    expect(() => validateContentEncoding(value)).toThrow(
      expect.objectContaining({ code: ErrorCode.InvalidParameter })
    )
  })

  it('removes transfer framing and identity without changing real codings', () => {
    expect(validateContentEncoding('identity, GZIP, Identity, br')).toBe('GZIP, br')
    expect(validateContentEncoding('GZIP, AWS-CHUNKED, br')).toBe('GZIP, br')
  })
})
