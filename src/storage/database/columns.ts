import { quoteIdentifier } from '@internal/database/sql'
import type { Bucket, IcebergCatalog, Obj, S3MultipartUpload } from '../schemas'

declare const selectColumnOptionsBrand: unique symbol

// A bitmask composed exclusively from declared select-column options.
export type SelectColumnOptionsMask = number & {
  readonly [selectColumnOptionsBrand]: true
}

export const SelectColumnOptions = Object.freeze({
  none: 0 as SelectColumnOptionsMask,
  excludeUserMetadata: (1 << 0) as SelectColumnOptionsMask,
  excludeMultipartMetadata: (1 << 1) as SelectColumnOptionsMask,
  excludeBucketType: (1 << 2) as SelectColumnOptionsMask,
  syntheticBucketType: (1 << 3) as SelectColumnOptionsMask,
} as const)

// Keep tokens interoperable when a test runner reloads this module in a second module registry.
const columnSelectionBrand: unique symbol = Symbol.for('storage.columnSelection')

// An immutable, precompiled SQL column selection for a specific row type.
export interface ColumnSelection<Row> {
  readonly [columnSelectionBrand]: (row: Row) => Row
}

export type ObjectColumnSelection = ColumnSelection<Obj>
export type BucketColumnSelection = ColumnSelection<Bucket>
export type MultipartColumnSelection = ColumnSelection<S3MultipartUpload>
export type AnalyticsColumnSelection = ColumnSelection<IcebergCatalog>

export type ObjectColumn = Exclude<keyof Obj, 'buckets'> | '*'
export type BucketColumn = keyof Bucket | '*'
export type MultipartColumn = keyof S3MultipartUpload | '*'
export type AnalyticsColumn = keyof IcebergCatalog | '*'

interface CompiledColumnSelection {
  readonly [columnSelectionBrand]: readonly string[]
}

interface CompiledColumn {
  readonly name: string
  readonly sql: string
}

const COLUMN_VARIANT_COUNT =
  Object.values(SelectColumnOptions).reduce((mask, option) => mask | option, 0) + 1

function defineColumns<Row>(columns: readonly string[]): ColumnSelection<Row> {
  // Exclusion options are matched by column name, not row type.
  // Callers must mask options to the relevant row type.
  // For example, excludeMultipartMetadata would also remove an object's metadata.
  const compiledColumns = columns.map<CompiledColumn>((name) => ({
    name,
    sql: name === '*' ? '*' : quoteIdentifier(name),
  }))
  const variants = new Array<string>(COLUMN_VARIANT_COUNT)

  for (let options = 0; options < COLUMN_VARIANT_COUNT; options++) {
    const selected: string[] = []
    let synthesizeBucketType = false

    for (const column of compiledColumns) {
      if (
        column.name === 'user_metadata' &&
        (options & SelectColumnOptions.excludeUserMetadata) !== 0
      ) {
        continue
      }
      if (
        column.name === 'metadata' &&
        (options & SelectColumnOptions.excludeMultipartMetadata) !== 0
      ) {
        continue
      }
      if (column.name === 'type') {
        if ((options & SelectColumnOptions.syntheticBucketType) !== 0) {
          synthesizeBucketType = true
          continue
        }
        if ((options & SelectColumnOptions.excludeBucketType) !== 0) {
          continue
        }
      }

      selected.push(column.sql)
    }

    if (synthesizeBucketType) {
      selected.push(`'STANDARD' AS "type"`)
    }

    variants[options] = selected.length === 0 ? quoteIdentifier('id') : selected.join(', ')
  }

  return Object.freeze({
    [columnSelectionBrand]: Object.freeze(variants),
  }) as unknown as ColumnSelection<Row>
}

export function defineObjectColumns<
  const Columns extends readonly [ObjectColumn, ...ObjectColumn[]],
>(...columns: Columns): ObjectColumnSelection {
  return defineColumns<Obj>(columns)
}

export function defineBucketColumns<
  const Columns extends readonly [BucketColumn, ...BucketColumn[]],
>(...columns: Columns): BucketColumnSelection {
  return defineColumns<Bucket>(columns)
}

export function defineMultipartColumns<
  const Columns extends readonly [MultipartColumn, ...MultipartColumn[]],
>(...columns: Columns): MultipartColumnSelection {
  return defineColumns<S3MultipartUpload>(columns)
}

export function defineAnalyticsColumns<
  const Columns extends readonly [AnalyticsColumn, ...AnalyticsColumn[]],
>(...columns: Columns): AnalyticsColumnSelection {
  return defineColumns<IcebergCatalog>(columns)
}

export function resolveColumns<Row>(
  selection: ColumnSelection<Row>,
  options: SelectColumnOptionsMask = 0 as SelectColumnOptionsMask
): string {
  return (selection as unknown as CompiledColumnSelection)[columnSelectionBrand][options]
}

export const OBJECT_ID_COLUMNS = defineObjectColumns('id')
export const OBJECT_ALL_COLUMNS = defineObjectColumns('*')
export const BUCKET_ID_COLUMNS = defineBucketColumns('id')
export const MULTIPART_ID_COLUMNS = defineMultipartColumns('id')
export const ANALYTICS_NAME_COLUMNS = defineAnalyticsColumns('name')
