import { SelectColumnOptions, selectColumns } from './columns'

describe('selectColumns', () => {
  test.each([
    ['id,version,metadata', '"id", "version", "metadata"'],
    [' id, version, , metadata ', '"id", "version", "metadata"'],
    ['', '"id"'],
    [' , ', '"id"'],
    ['*,id', '*, "id"'],
  ])('compiles %j into a quoted SELECT list', (columns, expected) => {
    expect(selectColumns(columns)).toBe(expected)
  })

  test('caches distinct migration-filtered variants of the same column list', () => {
    const columns = 'id,user_metadata,metadata'

    expect(selectColumns(columns)).toBe('"id", "user_metadata", "metadata"')
    expect(selectColumns(columns, SelectColumnOptions.excludeUserMetadata)).toBe('"id", "metadata"')
    expect(
      selectColumns(
        columns,
        SelectColumnOptions.excludeUserMetadata | SelectColumnOptions.excludeMultipartMetadata
      )
    ).toBe('"id"')
    expect(selectColumns(columns)).toBe('"id", "user_metadata", "metadata"')
  })

  test('falls back to id when every requested bucket column is unavailable', () => {
    expect(selectColumns('type', SelectColumnOptions.excludeBucketType)).toBe('"id"')
  })

  test('keeps the existing synthetic bucket type at the end of the SELECT list', () => {
    expect(selectColumns('type,id,name', SelectColumnOptions.syntheticBucketType)).toBe(
      '"id", "name", \'STANDARD\' AS "type"'
    )
    expect(selectColumns('type', SelectColumnOptions.syntheticBucketType)).toBe(
      '\'STANDARD\' AS "type"'
    )
    expect(selectColumns('type,', SelectColumnOptions.syntheticBucketType)).toBe(
      '"id", \'STANDARD\' AS "type"'
    )
  })

  test('still rejects invalid PostgreSQL identifiers', () => {
    expect(() => selectColumns('id,metadata->>key')).toThrow(
      'Invalid PostgreSQL identifier: metadata->>key'
    )
  })
})
