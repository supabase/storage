import buildSerializer, { type Schema } from 'fast-json-stringify'
import { expect, expectTypeOf, it, vi } from 'vitest'
import type { Database } from '../database/adapter'
import type { normalizeLifecycleConfiguration } from '../lifecycle/configuration'
import type { Storage } from '../storage'
import {
  type BucketLifecycleConfiguration,
  bucketLifecycleConfigurationReadSchema,
  type LifecycleRule,
  type NoncurrentVersionExpiration,
  type StoredBucketLifecycleConfiguration,
  type StoredLifecycleRule,
} from './lifecycle'

it('serializes both stored selectors without strict schema warnings', () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  try {
    // The serializer's schema types require mutable arrays; our schemas are readonly.
    const serialize = buildSerializer(bucketLifecycleConfigurationReadSchema as unknown as Schema)
    const configuration = {
      rules: [
        { id: 'filter', status: 'Enabled', filter: {} },
        { id: 'prefix', status: 'Disabled', legacyPrefix: '' },
      ],
    }
    expect(JSON.parse(serialize(configuration))).toEqual(configuration)
    expect(warn).not.toHaveBeenCalled()
  } finally {
    warn.mockRestore()
  }
})

it('keeps normalized rules stricter than stored rules', () => {
  expectTypeOf<
    LifecycleRule['noncurrentVersionExpiration']
  >().toEqualTypeOf<NoncurrentVersionExpiration>()
  expectTypeOf<LifecycleRule['filter']>().toEqualTypeOf<Record<string, never> | undefined>()
  expectTypeOf<StoredLifecycleRule['noncurrentVersionExpiration']>().toEqualTypeOf<
    NoncurrentVersionExpiration | undefined
  >()
  expectTypeOf<StoredLifecycleRule['filter']>().toEqualTypeOf<Record<string, unknown> | undefined>()
  expectTypeOf<BucketLifecycleConfiguration>().toExtend<StoredBucketLifecycleConfiguration>()
  expectTypeOf<StoredBucketLifecycleConfiguration>().not.toExtend<BucketLifecycleConfiguration>()
})

it('requires normalized configuration at the write boundaries', () => {
  expectTypeOf<
    ReturnType<typeof normalizeLifecycleConfiguration>
  >().toEqualTypeOf<BucketLifecycleConfiguration>()
  expectTypeOf<
    Parameters<Database['putLifecycleConfiguration']>[1]
  >().toEqualTypeOf<BucketLifecycleConfiguration>()
  expectTypeOf<
    Parameters<Storage['putBucketLifecycle']>[1]
  >().toEqualTypeOf<BucketLifecycleConfiguration>()
})

it('returns stored configuration at the read boundaries', () => {
  expectTypeOf<
    Awaited<ReturnType<Database['findLifecycleBucket']>>['lifecycle_configuration']
  >().toEqualTypeOf<StoredBucketLifecycleConfiguration | null>()
  expectTypeOf<
    Awaited<ReturnType<Storage['getBucketLifecycle']>>
  >().toEqualTypeOf<StoredBucketLifecycleConfiguration | null>()
})
