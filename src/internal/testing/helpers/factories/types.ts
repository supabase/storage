/**
 * Tracks everything a test file created so the after-all hook can wipe it in
 * a small number of bulk DELETEs. We intentionally record only the *minimum*
 * identifiers needed: bucket ids, object (bucket_id, name) pairs, user ids.
 */
export interface CleanupRegistry {
  buckets: Set<string>
  users: Set<string>
  /** Full S3 keys to delete from the backend after the DB cleanup. */
  s3Keys: Set<string>
}

export function createRegistry(): CleanupRegistry {
  return { buckets: new Set(), users: new Set(), s3Keys: new Set() }
}
