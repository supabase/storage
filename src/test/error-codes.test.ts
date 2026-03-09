import { ERRORS, ErrorCode, StorageBackendError } from '@internal/errors'

describe('ERRORS.InvalidKey', () => {
  it('does not throw for unpaired high surrogates', () => {
    const malformedKey = 'bad-\uD800-key'

    expect(() => ERRORS.InvalidKey(malformedKey)).not.toThrow()

    const error = ERRORS.InvalidKey(malformedKey)
    expect(error).toBeInstanceOf(StorageBackendError)
    expect(error.code).toBe(ErrorCode.InvalidKey)
    expect(error.httpStatusCode).toBe(400)
    expect(error.message).toBe('Invalid key: bad-%EF%BF%BD-key')
  })

  it('does not throw for unpaired low surrogates', () => {
    const malformedKey = 'bad-\uDC00-key'

    expect(() => ERRORS.InvalidKey(malformedKey)).not.toThrow()

    const error = ERRORS.InvalidKey(malformedKey)
    expect(error.code).toBe(ErrorCode.InvalidKey)
    expect(error.httpStatusCode).toBe(400)
    expect(error.message).toBe('Invalid key: bad-%EF%BF%BD-key')
  })

  it('encodes valid Unicode and reserved characters in InvalidKey messages', () => {
    const malformedKey = 'bad-일이삼/🙂?#%.png'

    const error = ERRORS.InvalidKey(malformedKey)

    expect(error.code).toBe(ErrorCode.InvalidKey)
    expect(error.httpStatusCode).toBe(400)
    expect(error.message).toBe(`Invalid key: ${encodeURIComponent(malformedKey)}`)
  })
})
