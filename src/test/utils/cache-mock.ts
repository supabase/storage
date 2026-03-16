export function mockCreateLruCache(overrides: Record<string, unknown>): void {
  jest.doMock('@internal/cache', () => {
    const actual = jest.requireActual('@internal/cache') as typeof import('@internal/cache')

    return {
      ...actual,
      createLruCache: ((optionsOrName: unknown, maybeOptions?: Record<string, unknown>) => {
        if (typeof optionsOrName === 'string') {
          return actual.createLruCache(
            optionsOrName as never,
            {
              ...(maybeOptions || {}),
              ...overrides,
            } as never
          )
        }

        return actual.createLruCache({
          ...(optionsOrName as Record<string, unknown>),
          ...overrides,
        } as never)
      }) as typeof actual.createLruCache,
    }
  })
}
