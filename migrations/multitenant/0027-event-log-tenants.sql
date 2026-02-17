CREATE TABLE IF NOT EXISTS event_log_tenants (
  tenant_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_polled_at TIMESTAMPTZ,
  next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  poll_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_event_log_tenants_next_poll ON event_log_tenants(next_poll_at);
