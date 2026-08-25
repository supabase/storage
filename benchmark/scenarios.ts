// Scenario definitions for the 5 functions wave-2 rewrote. Each scenario calls
// the function directly via named-parameter SQL (`func(param => value, ...)`),
// not through pg.ts's app-level methods - the app layer routes delimiter-based
// listing through storage.search_v2() whenever the 'search-v2' migration is
// present (true for both PRE and POST states here, since search-v2 predates
// object-versioning-core by a wide margin), which would make
// list_objects_with_delimiter effectively unreachable through the normal call
// path. Calling all 5 functions directly guarantees each one actually gets
// exercised, regardless of app-level routing.
//
// All 4 non-aggregate functions share identical leading parameter names
// between their pre- and post-migration signatures - wave-2 only appended new
// trailing params with defaults. `versioningArgs` are appended only when
// benchmarking a POST-state dataset; the same scenario definition works for
// PRE by simply omitting them (the old function doesn't have the parameters,
// so passing them would error - if it's not in the DB, it's not requested).

export interface FunctionCall {
  fn: string
  args: Record<string, string>
  /** Only appended when running against a POST-migration dataset. */
  versioningArgs?: Record<string, string>
}

export interface Scenario {
  name: string
  /** Which seeded datasets this scenario is valid against. */
  appliesTo: Array<'pre' | 'post-default' | 'post-versioned'>
  call: FunctionCall
}

const BUCKET = `'benchmark'`

export const SCENARIOS: Scenario[] = [
  // --- list_objects_with_delimiter ---
  {
    name: 'list_objects_with_delimiter: root, small page',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '100',
      },
    },
  },
  {
    name: 'list_objects_with_delimiter: root, large page (batch-algorithm stress)',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '10000',
      },
    },
  },
  {
    name: 'list_objects_with_delimiter: within one busy folder',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `'folder-00001/'`,
        delimiter_param: `'/'`,
        max_keys: '1000',
      },
    },
  },
  {
    name: 'list_objects_with_delimiter: root, desc sort',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '1000',
        sort_order: `'desc'`,
      },
    },
  },
  {
    name: 'list_objects_with_delimiter: noncurrentVersions=include, small page',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '100',
      },
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },
  {
    name: 'list_objects_with_delimiter: noncurrentVersions=include, large page',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '10000',
      },
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },
  {
    name: 'list_objects_with_delimiter: deleteMarkers=only',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.list_objects_with_delimiter',
      args: {
        _bucket_id: BUCKET,
        prefix_param: `''`,
        delimiter_param: `'/'`,
        max_keys: '1000',
      },
      versioningArgs: { delete_markers: `'only'` },
    },
  },

  // --- search (offset-paginated) ---
  {
    name: 'search: small page, offset 0',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search',
      args: { prefix: `''`, bucketname: BUCKET, limits: '100', offsets: '0' },
    },
  },
  {
    name: 'search: large page',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search',
      args: { prefix: `''`, bucketname: BUCKET, limits: '10000', offsets: '0' },
    },
  },
  {
    name: 'search: deep offset',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search',
      args: { prefix: `''`, bucketname: BUCKET, limits: '100', offsets: '5000' },
    },
  },
  {
    name: 'search: noncurrentVersions=include, large page',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.search',
      args: { prefix: `''`, bucketname: BUCKET, limits: '10000', offsets: '0' },
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },

  // --- search_by_timestamp ---
  {
    name: 'search_by_timestamp: small page, updated_at asc',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search_by_timestamp',
      args: {
        p_prefix: `''`,
        p_bucket_id: BUCKET,
        p_limit: '100',
        p_level: '1',
        p_start_after: `''`,
        p_sort_order: `'asc'`,
        p_sort_column: `'updated_at'`,
        p_sort_column_after: `''`,
      },
    },
  },
  {
    name: 'search_by_timestamp: large page',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search_by_timestamp',
      args: {
        p_prefix: `''`,
        p_bucket_id: BUCKET,
        p_limit: '10000',
        p_level: '1',
        p_start_after: `''`,
        p_sort_order: `'asc'`,
        p_sort_column: `'updated_at'`,
        p_sort_column_after: `''`,
      },
    },
  },
  {
    name: 'search_by_timestamp: noncurrentVersions=include, large page',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.search_by_timestamp',
      args: {
        p_prefix: `''`,
        p_bucket_id: BUCKET,
        p_limit: '10000',
        p_level: '1',
        p_start_after: `''`,
        p_sort_order: `'asc'`,
        p_sort_column: `'updated_at'`,
        p_sort_column_after: `''`,
      },
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },

  // --- search_v2 ---
  {
    name: 'search_v2: root, small page',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search_v2',
      args: { prefix: `''`, bucket_name: BUCKET, limits: '100', levels: '1' },
    },
  },
  {
    name: 'search_v2: root, large page',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: {
      fn: 'storage.search_v2',
      args: { prefix: `''`, bucket_name: BUCKET, limits: '10000', levels: '1' },
    },
  },
  {
    name: 'search_v2: noncurrentVersions=include, large page',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.search_v2',
      args: { prefix: `''`, bucket_name: BUCKET, limits: '10000', levels: '1' },
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },

  // --- get_size_by_bucket ---
  {
    name: 'get_size_by_bucket: default',
    appliesTo: ['pre', 'post-default', 'post-versioned'],
    call: { fn: 'storage.get_size_by_bucket', args: {} },
  },
  {
    name: 'get_size_by_bucket: noncurrentVersions=include',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.get_size_by_bucket',
      args: {},
      versioningArgs: { noncurrent_versions: `'include'` },
    },
  },
  {
    name: 'get_size_by_bucket: deleteMarkers=only',
    appliesTo: ['post-versioned'],
    call: {
      fn: 'storage.get_size_by_bucket',
      args: {},
      versioningArgs: { delete_markers: `'only'` },
    },
  },
]

export function buildCallSql(call: FunctionCall, includeVersioningArgs: boolean): string {
  const args = { ...call.args, ...(includeVersioningArgs ? call.versioningArgs : {}) }
  const argList = Object.entries(args)
    .map(([name, value]) => `${name} => ${value}`)
    .join(', ')
  return `SELECT * FROM ${call.fn}(${argList})`
}
