ALTER TABLE storage.buckets DROP CONSTRAINT IF EXISTS buckets_versioning_dark_check;

CREATE OR REPLACE FUNCTION storage.enforce_versioning_status_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.versioning_status = OLD.versioning_status THEN
        RETURN NEW;
    END IF;

    IF NEW.versioning_status = 'DISABLED' OR
       (OLD.versioning_status = 'DISABLED' AND NEW.versioning_status = 'SUSPENDED') THEN
        RAISE EXCEPTION 'Cannot transition bucket versioning status from % to %',
            OLD.versioning_status, NEW.versioning_status;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS enforce_versioning_status_transition ON storage.buckets;
CREATE TRIGGER enforce_versioning_status_transition
    BEFORE UPDATE OF versioning_status ON storage.buckets
    FOR EACH ROW
    EXECUTE FUNCTION storage.enforce_versioning_status_transition();
