CREATE TABLE IF NOT EXISTS lifecycle_tenants (
    tenant_id text PRIMARY KEY
      REFERENCES tenants(id)
      ON DELETE CASCADE,

    next_dispatch_at timestamptz NOT NULL DEFAULT now(),

    claim_id uuid,
    claim_until timestamptz,

    last_dispatched_at timestamptz,
    last_completed_at timestamptz,
    last_success_at timestamptz,

    failure_count integer NOT NULL DEFAULT 0,
    last_error jsonb,

    CONSTRAINT lifecycle_tenants_failure_count_check
      CHECK (failure_count >= 0)
);

CREATE INDEX IF NOT EXISTS lifecycle_tenants_due_idx
ON lifecycle_tenants (
    next_dispatch_at,
    tenant_id
);

CREATE INDEX IF NOT EXISTS lifecycle_tenants_expired_claim_idx
ON lifecycle_tenants (
    claim_until,
    tenant_id
)
WHERE claim_until IS NOT NULL;
