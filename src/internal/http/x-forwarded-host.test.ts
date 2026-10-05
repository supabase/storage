async function loadXForwardedHostRegExp({
  isMultitenant,
  pattern,
}: {
  isMultitenant: boolean
  pattern?: string
}) {
  vi.resetModules()

  vi.stubEnv('MULTI_TENANT', isMultitenant ? 'true' : 'false')
  vi.stubEnv('IS_MULTITENANT', isMultitenant ? 'true' : 'false')
  vi.stubEnv('X_FORWARDED_HOST_REGEXP', '')
  vi.stubEnv('REQUEST_X_FORWARDED_HOST_REGEXP', pattern ?? '')

  return await import('./x-forwarded-host')
}

afterEach(() => {
  vi.resetModules()
})

describe('getXForwardedHostRegExp', () => {
  it('skips compiling the host pattern when multitenancy is disabled', async () => {
    const { getXForwardedHostRegExp } = await loadXForwardedHostRegExp({
      isMultitenant: false,
      pattern: '[',
    })

    expect(getXForwardedHostRegExp()).toBeUndefined()
  })

  it('returns undefined when no host pattern is configured', async () => {
    const { getXForwardedHostRegExp } = await loadXForwardedHostRegExp({
      isMultitenant: true,
    })

    expect(getXForwardedHostRegExp()).toBeUndefined()
  })

  it('reuses the compiled regexp from startup config', async () => {
    const { getXForwardedHostRegExp } = await loadXForwardedHostRegExp({
      isMultitenant: true,
      pattern: '^([a-z]+)\\.local$',
    })

    const first = getXForwardedHostRegExp()
    const second = getXForwardedHostRegExp()

    expect(second).toBe(first)
    expect('tenant.local'.match(first!)).toBeTruthy()
  })

  it('does not recompile when config is reloaded after module load', async () => {
    const { getXForwardedHostRegExp } = await loadXForwardedHostRegExp({
      isMultitenant: true,
      pattern: '^([a-z]+)\\.local$',
    })
    const previous = getXForwardedHostRegExp()

    vi.stubEnv('REQUEST_X_FORWARDED_HOST_REGEXP', '^([0-9]+)\\.local$')
    const { getConfig } = await import('../../config')
    getConfig({ reload: true })

    const current = getXForwardedHostRegExp()

    expect(current).toBe(previous)
    expect('tenant.local'.match(current!)).toBeTruthy()
    expect('123.local'.match(current!)).toBeFalsy()
  })

  it('throws while loading the helper when the configured pattern is invalid', async () => {
    await expect(
      loadXForwardedHostRegExp({
        isMultitenant: true,
        pattern: '[',
      })
    ).rejects.toThrow(SyntaxError)
  })
})
