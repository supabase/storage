DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE c.conname = 'objects_name_check'
      AND n.nspname = 'storage'
      AND t.relname = 'objects'
  ) THEN
    ALTER TABLE "storage"."objects"
      ADD CONSTRAINT objects_name_check
      CHECK (
        name !~ E'[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'
        AND POSITION(U&'\FFFE' IN name) = 0
        AND POSITION(U&'\FFFF' IN name) = 0
      );
  END IF;
END
$$;
