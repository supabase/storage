CREATE TABLE IF NOT EXISTS storage.event_log (
  id BIGSERIAL PRIMARY KEY,
  event_name TEXT NOT NULL,
  payload JSONB NOT NULL,
  send_options JSONB,
  signature TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_log_status_id ON storage.event_log(status, id) WHERE status = 'PENDING';
