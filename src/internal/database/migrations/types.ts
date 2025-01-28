export const DBMigration = {
  initialmigration: 0,
  'search-files-search-function': 1,
  'storage-schema': 2,
  'pathtoken-column': 3,
  'add-migrations-rls': 4,
  'add-size-functions': 5,
  'change-column-name-in-get-size': 6,
  'add-rls-to-buckets': 7,
  'add-public-to-buckets': 8,
  'fix-search-function': 9,
  'add-trigger-to-auto-update-updated_at-column': 10,
  'add-automatic-avif-detection-flag': 11,
  'add-bucket-custom-limits': 12,
  'use-bytes-for-max-size': 13,
  'add-can-insert-object-function': 14,
  'add-version': 15,
  'drop-owner-foreign-key': 16,
  add_owner_id_column_deprecate_owner: 17,
  'alter-default-value-objects-id': 18,
  'list-objects-with-delimiter': 19,
  's3-multipart-uploads': 20,
  's3-multipart-uploads-big-ints': 21,
  'optimize-search-function': 22,
  'operation-function': 23,
  'custom-metadata': 24,
  'unicode-object-names': 25,
} as const
