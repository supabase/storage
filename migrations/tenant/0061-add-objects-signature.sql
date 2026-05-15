ALTER TABLE storage.objects
    ADD COLUMN IF NOT EXISTS signature bytea;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'objects_signature_length'
          AND conrelid = 'storage.objects'::regclass
    ) THEN
        ALTER TABLE storage.objects
            ADD CONSTRAINT objects_signature_length
            CHECK (signature IS NULL OR octet_length(signature) = 32);
    END IF;
END $$;
