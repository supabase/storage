import { describe, expect, it } from 'vitest'
import { readConfig } from './config.js'
import { DestinationResolver } from './destinations.js'

describe('database destination resolution', () => {
  it('derives single-tenant external pool state from parsed config', async () => {
    const resolver = new DestinationResolver(
      readConfig({
        DATABASE_POOL_URL: 'postgres://pooler',
      })
    )

    const destination = await resolver.resolve('default')

    expect(destination).toMatchObject({
      connectionString: 'postgres://pooler',
      isExternalPool: true,
    })
  })
})
