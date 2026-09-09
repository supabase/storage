-- Advisory snapshots collected by tenant dispatchers; timestamps expose stale or
-- unavailable tenant observations. Scheduling continues to use the tenant state.
ALTER TABLE lifecycle_tenants
  ADD COLUMN IF NOT EXISTS max_shard_failure_count integer,
  ADD COLUMN IF NOT EXISTS oldest_in_flight_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS shard_status_observed_at timestamptz;
