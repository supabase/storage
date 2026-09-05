import buildSerializer, { type Schema } from 'fast-json-stringify'
import { expect, expectTypeOf, it, vi } from 'vitest'
import type { Database } from '../database/adapter'
import type { normalizeLifecycleConfiguration } from '../lifecycle/configuration'
import type { Storage } from '../storage'
import {
  type BucketLifecycleConfiguration,
  bucketLifecycleConfigurationSchema,
  type LifecycleRule,
  type NoncurrentVersionExpiration,
} from './lifecycle'

it('serializes valid configurations without strict schema warnings', () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  try {
    // The serializer's schema types require mutable arrays; our schemas are readonly.
    const serialize = buildSerializer(bucketLifecycleConfigurationSchema as unknown as Schema)
    const configuration = {
      rules: [
        {
          id: 'filter',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
        {
          id: 'prefix',
          status: 'Disabled',
          legacyPrefix: '',
          noncurrentVersionExpiration: { noncurrentDays: 7 },
        },
      ],
    }
    expect(JSON.parse(serialize(configuration))).toEqual(configuration)
    expect(warn).not.toHaveBeenCalled()
  } finally {
    warn.mockRestore()
  }
})

it('requires expiration and exactly one selector in normalized rules', () => {
  expectTypeOf<
    LifecycleRule['noncurrentVersionExpiration']
  >().toEqualTypeOf<NoncurrentVersionExpiration>()
  expectTypeOf<{ status: 'Enabled'; filter: {} }>().not.toExtend<LifecycleRule>()
  expectTypeOf<{
    status: 'Enabled'
    noncurrentVersionExpiration: NoncurrentVersionExpiration
  }>().not.toExtend<LifecycleRule>()
  expectTypeOf<{
    status: 'Enabled'
    filter: {}
    legacyPrefix: ''
    noncurrentVersionExpiration: NoncurrentVersionExpiration
  }>().not.toExtend<LifecycleRule>()
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

it('returns normalized configuration at the read boundaries', () => {
  expectTypeOf<
    Awaited<ReturnType<Database['findLifecycleBucket']>>['lifecycle_configuration']
  >().toEqualTypeOf<BucketLifecycleConfiguration | null>()
  expectTypeOf<
    Awaited<ReturnType<Storage['getBucketLifecycle']>>
  >().toEqualTypeOf<BucketLifecycleConfiguration | null>()
})
