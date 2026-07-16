import { quoteIdentifier } from '@internal/database'

export const SelectColumnOptions = {
  none: 0,
  excludeUserMetadata: 1 << 0,
  excludeMultipartMetadata: 1 << 1,
  excludeBucketType: 1 << 2,
  syntheticBucketType: 1 << 3,
} as const

const DEFAULT_SELECT_COLUMNS = '"id"'
const SYNTHETIC_BUCKET_TYPE = `'STANDARD' AS "type"`
const SELECT_COLUMNS_CACHE_LIMIT = 64
const MAX_CACHEABLE_COLUMN_LIST_LENGTH = 256

// The Database API receives a small, stable set of internal CSV literals. Cache
// each compiled migration variant so hot queries pay one Map lookup rather than
// repeatedly splitting, trimming, validating, quoting, and joining the same list.
// The limits keep accidental dynamic callers from turning this into an unbounded cache.
const selectColumnsCache = new Map<string, Array<string | undefined>>()

export function selectColumns(columns: string, options: number = SelectColumnOptions.none): string {
  const variants = selectColumnsCache.get(columns)
  const cached = variants?.[options]

  if (cached !== undefined) {
    return cached
  }

  const selected: string[] = []
  let addSyntheticBucketType = false
  let requestedRealBucketColumn = false
  const excludeUserMetadata = (options & SelectColumnOptions.excludeUserMetadata) !== 0
  const excludeMultipartMetadata = (options & SelectColumnOptions.excludeMultipartMetadata) !== 0
  const excludeBucketType = (options & SelectColumnOptions.excludeBucketType) !== 0
  const useSyntheticBucketType = (options & SelectColumnOptions.syntheticBucketType) !== 0

  for (const value of columns.split(',')) {
    const column = value.trim()
    if (column.length === 0) {
      // listBuckets historically treats an empty non-type segment as an id
      // fallback even when a synthetic type was also requested.
      requestedRealBucketColumn ||= useSyntheticBucketType
      continue
    }

    if (column === 'user_metadata' && excludeUserMetadata) {
      continue
    }
    if (column === 'metadata' && excludeMultipartMetadata) {
      continue
    }
    if (column === 'type') {
      if (useSyntheticBucketType) {
        addSyntheticBucketType = true
        continue
      }
      if (excludeBucketType) {
        continue
      }
    }

    requestedRealBucketColumn = true
    selected.push(column === '*' ? '*' : quoteIdentifier(column))
  }

  if (addSyntheticBucketType) {
    // Preserve listBuckets' existing behavior: requested synthetic type follows
    // every real column, independent of its position in the input CSV.
    if (selected.length === 0 && requestedRealBucketColumn) {
      selected.push(DEFAULT_SELECT_COLUMNS)
    }
    selected.push(SYNTHETIC_BUCKET_TYPE)
  }

  const sql = selected.length ? selected.join(', ') : DEFAULT_SELECT_COLUMNS

  if (variants) {
    variants[options] = sql
  } else if (
    selectColumnsCache.size < SELECT_COLUMNS_CACHE_LIMIT &&
    columns.length <= MAX_CACHEABLE_COLUMN_LIST_LENGTH
  ) {
    const newVariants: Array<string | undefined> = []
    newVariants[options] = sql
    selectColumnsCache.set(columns, newVariants)
  }

  return sql
}
