import { getXForwardedHostRegExp } from './x-forwarded-host'

describe('getXForwardedHostRegExp', () => {
  it('skips compiling the host pattern when multitenancy is disabled', () => {
    expect(
      getXForwardedHostRegExp({
        isMultitenant: false,
        requestXForwardedHostRegExp: '[',
      })
    ).toBeUndefined()
  })

  it('reuses the compiled regexp for the same pattern', () => {
    const config = {
      isMultitenant: true,
      requestXForwardedHostRegExp: '^([a-z]+)\\.local$',
    }

    expect(getXForwardedHostRegExp(config)).toBe(getXForwardedHostRegExp(config))
  })

  it('recompiles when the configured pattern changes', () => {
    const first = getXForwardedHostRegExp({
      isMultitenant: true,
      requestXForwardedHostRegExp: '^([a-z]+)\\.local$',
    })
    const second = getXForwardedHostRegExp({
      isMultitenant: true,
      requestXForwardedHostRegExp: '^([0-9]+)\\.local$',
    })

    expect(second).not.toBe(first)
    expect('123.local'.match(second!)).toBeTruthy()
  })

  it('does not replace the cache when a new pattern is invalid', () => {
    const previous = getXForwardedHostRegExp({
      isMultitenant: true,
      requestXForwardedHostRegExp: '^([a-z]+)\\.local$',
    })

    expect(() =>
      getXForwardedHostRegExp({
        isMultitenant: true,
        requestXForwardedHostRegExp: '[',
      })
    ).toThrow(SyntaxError)
    expect(() =>
      getXForwardedHostRegExp({
        isMultitenant: true,
        requestXForwardedHostRegExp: '[',
      })
    ).toThrow(SyntaxError)

    expect(
      getXForwardedHostRegExp({
        isMultitenant: true,
        requestXForwardedHostRegExp: '^([a-z]+)\\.local$',
      })
    ).toBe(previous)
  })
})
