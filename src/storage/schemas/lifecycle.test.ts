import { expectTypeOf, it } from 'vitest'
import type { Database } from '../database/adapter'
import type { normalizeLifecycleConfiguration } from '../lifecycle/configuration'
import type { Storage } from '../storage'
import type {
  BucketLifecycleConfiguration,
  LifecycleRule,
  NoncurrentVersionExpiration,
  StoredBucketLifecycleConfiguration,
  StoredLifecycleRule,
} from './lifecycle'

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
