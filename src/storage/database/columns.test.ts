import {
  defineAnalyticsColumns,
  defineBucketColumns,
  defineMultipartColumns,
  defineObjectColumns,
  resolveColumns,
  SelectColumnOptions,
  type SelectColumnOptionsMask,
} from './columns'

describe('compiled column selections', () => {
  test('compiles and quotes columns once in declaration order', () => {
    const columns = defineObjectColumns('name', 'bucket_id', 'user_metadata')

    expect(resolveColumns(columns)).toBe('"name", "bucket_id", "user_metadata"')
  })

  test('preserves the wildcard', () => {
    expect(resolveColumns(defineObjectColumns('*'))).toBe('*')
  })

  test('precompiles migration-compatible object columns', () => {
    const columns = defineObjectColumns('name', 'user_metadata', 'metadata')

    expect(resolveColumns(columns, SelectColumnOptions.excludeUserMetadata)).toBe(
      '"name", "metadata"'
    )
  })

  test('precompiles every multipart migration combination', () => {
    const columns = defineMultipartColumns('id', 'user_metadata', 'metadata')

    expect(resolveColumns(columns, SelectColumnOptions.excludeUserMetadata)).toBe(
      '"id", "metadata"'
    )
    expect(resolveColumns(columns, SelectColumnOptions.excludeMultipartMetadata)).toBe(
      '"id", "user_metadata"'
    )
    expect(
      resolveColumns(
        columns,
        (SelectColumnOptions.excludeUserMetadata |
          SelectColumnOptions.excludeMultipartMetadata) as SelectColumnOptionsMask
      )
    ).toBe('"id"')
  })

  test('precompiles physical and synthetic bucket type variants', () => {
    const columns = defineBucketColumns('id', 'type', 'name')

    expect(resolveColumns(columns, SelectColumnOptions.excludeBucketType)).toBe('"id", "name"')
    expect(resolveColumns(columns, SelectColumnOptions.syntheticBucketType)).toBe(
      '"id", "name", \'STANDARD\' AS "type"'
    )
  })

  test('precompiles the maximum declared option mask', () => {
    const allOptions = Object.values(SelectColumnOptions).reduce(
      (mask, option) => mask | option,
      0
    ) as SelectColumnOptionsMask

    expect(resolveColumns(defineObjectColumns('id'), allOptions)).toBe('"id"')
  })

  test('accepts declared option masks but not arbitrary numbers', () => {
    expectTypeOf<SelectColumnOptionsMask>().toExtend<number>()
    expectTypeOf<number>().not.toExtend<SelectColumnOptionsMask>()
    expectTypeOf(resolveColumns).parameter(1).toEqualTypeOf<SelectColumnOptionsMask | undefined>()
  })

  test('falls back to id if a migration removes every selected physical column', () => {
    const objectColumns = defineObjectColumns('user_metadata')
    const bucketColumns = defineBucketColumns('type')

    expect(resolveColumns(objectColumns, SelectColumnOptions.excludeUserMetadata)).toBe('"id"')
    expect(resolveColumns(bucketColumns, SelectColumnOptions.excludeBucketType)).toBe('"id"')
    expect(resolveColumns(bucketColumns, SelectColumnOptions.syntheticBucketType)).toBe(
      '\'STANDARD\' AS "type"'
    )
  })

  test('supports analytics columns independently from storage buckets', () => {
    expect(resolveColumns(defineAnalyticsColumns('name', 'deleted_at'))).toBe(
      '"name", "deleted_at"'
    )
  })

  test('rejects invalid identifiers while defining a token', () => {
    expect(() => defineObjectColumns('name; DROP TABLE objects' as 'name')).toThrow(
      'Invalid PostgreSQL identifier'
    )
  })

  test('returns immutable tokens', () => {
    const columns = defineObjectColumns('id')

    expect(Object.isFrozen(columns)).toBe(true)
    expect(Object.keys(columns)).toEqual([])
  })

  test('resolves tokens created by another module instance', async () => {
    const columns = defineObjectColumns('id', 'name')

    vi.resetModules()
    const reloadedColumns = await import('./columns')

    expect(reloadedColumns.resolveColumns(columns)).toBe('"id", "name"')
  })
})
