import { ErrorCode } from '@internal/errors'
import { validateMimeType } from './uploader'

describe('validateMimeType', () => {
  it('accepts an exact media type match', () => {
    expect(validateMimeType('image/png', ['image/png'])).toBe(true)
  })

  it('accepts a subtype wildcard', () => {
    expect(validateMimeType('image/png', ['image/*'])).toBe(true)
  })

  it('ignores media type parameters when matching', () => {
    expect(validateMimeType('text/plain;charset=UTF-8', ['text/plain'])).toBe(true)
  })

  it('ignores media type parameters with surrounding whitespace', () => {
    expect(validateMimeType('application/json; charset=utf-8', ['application/json'])).toBe(true)
  })

  it('ignores media type parameters when matching a subtype wildcard', () => {
    expect(validateMimeType('text/plain;charset=UTF-8', ['text/*'])).toBe(true)
  })

  it('ignores parameters declared on the allowed media type', () => {
    expect(validateMimeType('text/plain', ['text/plain;charset=UTF-8'])).toBe(true)
  })

  it('still rejects a media type that is not allowed', () => {
    expectInvalidMimeType(() => validateMimeType('image/png;foo=bar', ['image/jpeg']))
  })

  it('rejects a malformed media type', () => {
    expectInvalidMimeType(() => validateMimeType('notamediatype', ['text/plain']))
  })
})

function expectInvalidMimeType(fn: () => unknown) {
  try {
    fn()
  } catch (error) {
    expect(error).toMatchObject({ code: ErrorCode.InvalidMimeType })
    return
  }

  throw new Error('expected an invalid mime type error')
}
