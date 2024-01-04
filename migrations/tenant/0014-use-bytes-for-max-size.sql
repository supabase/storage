ALTER TABLE storage.buckets RENAME COLUMN max_file_size_kb TO file_size_limit;
ALTER TABLE storage.buckets ALTER COLUMN file_size_limit TYPE bigint;