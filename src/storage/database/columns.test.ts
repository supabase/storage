import { DBMigration } from '@internal/database/migrations/types'
import {
  type ColumnSetState,
  defineColumnSet,
  prepareColumnState,
  resolveColumns,
  staticSqlLiteral,
} from './column-set'
import { analyticsColumns, bucketColumns, multipartColumns, objectColumns } from './columns'

declare const firstColumnSetId: unique symbol
declare const secondColumnSetId: unique symbol

describe('compiled column selections', () => {
  test('compiles and quotes columns once in declaration order', () => {
    const columns = objectColumns.select('name', 'bucket_id', 'user_metadata')
    const state = prepareColumnState(objectColumns, DBMigration['custom-metadata'])

    expect(resolveColumns(columns, state)).toBe('"name", "bucket_id", "user_metadata"')
  })

  test('preserves the wildcard', () => {
    const columns = objectColumns.select('*')
    const state = prepareColumnState(objectColumns, DBMigration['operation-function'])

    expect(resolveColumns(columns, state)).toBe('*')
  })

  test('applies object rules without affecting same-named columns', () => {
    const columns = objectColumns.select('name', 'user_metadata', 'metadata')
    const legacyState = prepareColumnState(objectColumns, DBMigration['operation-function'])
    const currentState = prepareColumnState(objectColumns, DBMigration['custom-metadata'])

    expect(resolveColumns(columns, legacyState)).toBe('"name", "metadata"')
    expect(resolveColumns(columns, currentState)).toBe('"name", "user_metadata", "metadata"')
  })

  test('derives only the ordered multipart migration layouts', () => {
    const columns = multipartColumns.select('id', 'user_metadata', 'metadata')
    const baselineState = prepareColumnState(multipartColumns, DBMigration['operation-function'])
    const customMetadataState = prepareColumnState(multipartColumns, DBMigration['custom-metadata'])
    const multipartMetadataState = prepareColumnState(
      multipartColumns,
      DBMigration['s3-multipart-uploads-metadata']
    )

    expect(resolveColumns(columns, baselineState)).toBe('"id"')
    expect(resolveColumns(columns, customMetadataState)).toBe('"id", "user_metadata"')
    expect(resolveColumns(columns, multipartMetadataState)).toBe(
      '"id", "user_metadata", "metadata"'
    )
  })

  test('keeps object metadata independent from multipart metadata', () => {
    const objectSelection = objectColumns.select('metadata')
    const multipartSelection = multipartColumns.select('metadata')
    const migration = DBMigration['custom-metadata']

    expect(resolveColumns(objectSelection, prepareColumnState(objectColumns, migration))).toBe(
      '"metadata"'
    )
    expect(
      resolveColumns(multipartSelection, prepareColumnState(multipartColumns, migration))
    ).toBe('"id"')
  })

  test('precompiles physical and synthetic bucket layouts', () => {
    const columns = bucketColumns.select('id', 'type', 'name')
    const legacyPhysical = prepareColumnState(bucketColumns, DBMigration['operation-function'])
    const currentPhysical = prepareColumnState(
      bucketColumns,
      DBMigration['iceberg-catalog-flag-on-buckets']
    )
    const legacySynthetic = prepareColumnState(
      bucketColumns,
      DBMigration['operation-function'],
      'synthetic'
    )
    const currentSynthetic = prepareColumnState(
      bucketColumns,
      DBMigration['iceberg-catalog-flag-on-buckets'],
      'synthetic'
    )

    expect(resolveColumns(columns, legacyPhysical)).toBe('"id", "name"')
    expect(resolveColumns(columns, currentPhysical)).toBe('"id", "type", "name"')
    expect(resolveColumns(columns, legacySynthetic)).toBe('"id", "name", \'STANDARD\' AS "type"')
    expect(resolveColumns(columns, currentSynthetic)).toBe('"id", "name", \'STANDARD\' AS "type"')
  })

  test('falls back to the table fallback if a migration removes every selected column', () => {
    const objectSelection = objectColumns.select('user_metadata')
    const bucketSelection = bucketColumns.select('type')
    const migration = DBMigration['operation-function']

    expect(resolveColumns(objectSelection, prepareColumnState(objectColumns, migration))).toBe(
      '"id"'
    )
    expect(resolveColumns(bucketSelection, prepareColumnState(bucketColumns, migration))).toBe(
      '"id"'
    )
    expect(
      resolveColumns(bucketSelection, prepareColumnState(bucketColumns, migration, 'synthetic'))
    ).toBe('\'STANDARD\' AS "type"')
  })

  test('supports analytics columns independently from storage buckets', () => {
    const columns = analyticsColumns.select('name', 'deleted_at')
    const state = prepareColumnState(analyticsColumns, DBMigration['operation-function'])

    expect(resolveColumns(columns, state)).toBe('"name", "deleted_at"')
  })

  test('adds a new table-local gate without a global option', () => {
    type ChecksummedRow = { id: string; checksum: string }

    const checksummedColumns = defineColumnSet<ChecksummedRow, typeof firstColumnSetId>({
      fallback: 'id',
      availableFrom: {
        checksum: 'custom-metadata',
      },
    })
    const columns = checksummedColumns.select('checksum')

    expect(
      resolveColumns(
        columns,
        prepareColumnState(checksummedColumns, DBMigration['operation-function'])
      )
    ).toBe('"id"')
    expect(
      resolveColumns(
        columns,
        prepareColumnState(checksummedColumns, DBMigration['custom-metadata'])
      )
    ).toBe('"checksum"')
  })

  test('encodes static synthetic literals instead of accepting raw SQL', () => {
    type LabelledRow = { id: string; label: string }

    const labelledColumns = defineColumnSet<LabelledRow, typeof firstColumnSetId>({
      fallback: 'id',
      synthetic: {
        label: staticSqlLiteral("O'Reilly"),
      },
    })
    const columns = labelledColumns.select('label')
    const state = prepareColumnState(labelledColumns, 0, 'synthetic')

    expect(resolveColumns(columns, state)).toBe("'O''Reilly' AS \"label\"")
  })

  test('rejects invalid and ambiguous declarations at module initialization', () => {
    expect(() => objectColumns.select('name; DROP TABLE objects' as 'name')).toThrow(
      'Invalid PostgreSQL identifier'
    )
    expect(() => objectColumns.select('id', 'id')).toThrow('Duplicate column')
    expect(() => objectColumns.select('*', 'id')).toThrow('Wildcard')
  })

  test('rejects table policies with an unavailable fallback or an unencoded literal', () => {
    type VersionedRow = { id: string; version: string }

    expect(() =>
      defineColumnSet<VersionedRow, typeof firstColumnSetId>({
        fallback: 'version',
        availableFrom: { version: 'custom-metadata' },
      })
    ).toThrow('Fallback column must be available in every migration')

    expect(() =>
      defineColumnSet<VersionedRow, typeof firstColumnSetId>({
        fallback: 'id',
        synthetic: { version: {} as never },
      })
    ).toThrow('Invalid static SQL literal')
  })

  test('returns immutable opaque tokens', () => {
    const columns = objectColumns.select('id')

    expect(Object.isFrozen(columns)).toBe(true)
    expect(Object.keys(columns)).toEqual([])
    expect(Object.getOwnPropertySymbols(columns)).toHaveLength(1)
  })

  test('brands table states instead of accepting arbitrary numbers', () => {
    expectTypeOf<ColumnSetState<typeof objectColumns>>().toExtend<number>()
    expectTypeOf<number>().not.toExtend<ColumnSetState<typeof objectColumns>>()
  })

  test('keeps selections and states tied to their originating set', () => {
    type SharedRow = { id: string; gated: string }

    const firstColumns = defineColumnSet<SharedRow, typeof firstColumnSetId>({ fallback: 'id' })
    const secondColumns = defineColumnSet<SharedRow, typeof secondColumnSetId>({
      fallback: 'id',
      availableFrom: { gated: 'custom-metadata' },
    })

    const firstSelection = firstColumns.select('id')
    const secondState = prepareColumnState(secondColumns, DBMigration['custom-metadata'])

    expectTypeOf(secondState).not.toExtend<ColumnSetState<typeof firstColumns>>()

    const resolveMismatchedSet = () => {
      // @ts-expect-error A state can resolve only selections from its originating set.
      return resolveColumns(firstSelection, secondState)
    }
    expectTypeOf(resolveMismatchedSet).returns.toBeString()
  })

  test('rejects invalid migration ordinals', () => {
    expect(() => prepareColumnState(objectColumns, -1)).toThrow(
      'Invalid database migration ordinal: -1'
    )
    expect(() => prepareColumnState(objectColumns, Number.NaN)).toThrow(
      'Invalid database migration ordinal: NaN'
    )
  })

  test('resolves tokens created by another module instance', async () => {
    const columns = objectColumns.select('id', 'name')
    const state = prepareColumnState(objectColumns, DBMigration['operation-function'])

    vi.resetModules()
    const reloadedColumnSet = await import('./column-set')

    expect(reloadedColumnSet.resolveColumns(columns, state)).toBe('"id", "name"')
  })
})
