alter table storage.buckets add column max_file_size_kb int default null;
alter table storage.buckets add column allowed_mime_types text[] default null;