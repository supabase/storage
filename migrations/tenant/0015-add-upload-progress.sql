CREATE TYPE upload_state AS ENUM ('STARTED', 'COMPLETED');

alter table storage.objects add column upload_state upload_state default null;
