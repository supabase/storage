ALTER TABLE public.lifecycle_tenants
ADD COLUMN IF NOT EXISTS wake_floor_at timestamptz;
