import { S3VectorsClient } from '@aws-sdk/client-s3vectors'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const config: { storageS3Region?: string; vectorBucketRegion?: string } = {}

vi.mock('../../../../config', () => ({
  getConfig: () => config,
}))

// The AWS SDK falls back to AWS_REGION/AWS_DEFAULT_REGION from the environment,
// which would mask a client that never received the configured region. These are
// cleared so the assertions below reflect the configuration only.
const AMBIENT_REGION_KEYS = ['AWS_REGION', 'AWS_DEFAULT_REGION'] as const

describe('createS3VectorClient', () => {
  let saved: Record<string, string | undefined>

  beforeEach(() => {
    saved = {}
    for (const key of AMBIENT_REGION_KEYS) {
      saved[key] = process.env[key]
      delete process.env[key]
    }
    config.storageS3Region = undefined
    config.vectorBucketRegion = undefined
    vi.resetModules()
  })

  afterEach(() => {
    for (const key of AMBIENT_REGION_KEYS) {
      if (saved[key] === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = saved[key]
      }
    }
    vi.resetModules()
  })

  it('returns a client configured with the vector bucket region', async () => {
    config.vectorBucketRegion = 'eu-west-1'
    config.storageS3Region = 'us-east-1'

    const { createS3VectorClient } = await import('./s3-vector')
    const client = createS3VectorClient()

    expect(client).toBeInstanceOf(S3VectorsClient)
    await expect(client.config.region()).resolves.toBe('eu-west-1')
  })

  it('falls back to the storage S3 region', async () => {
    config.storageS3Region = 'sa-east-1'

    const { createS3VectorClient } = await import('./s3-vector')
    const client = createS3VectorClient()

    await expect(client.config.region()).resolves.toBe('sa-east-1')
  })
})
