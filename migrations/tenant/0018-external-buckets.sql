CREATE TABLE bucket_credentials (
      "id" uuid NOT NULL DEFAULT extensions.uuid_generate_v4(),
      name text NOT NULL unique,
      access_key text NULL,
      secret_key text NULL,
      role text null,
      region text not null,
      endpoint text NULL,
      force_path_style boolean NOT NULL default false,
      PRIMARY KEY (id)
);

ALTER TABLE storage.buckets ADD COLUMN credential_id uuid DEFAULT NULL;
ALTER TABLE storage.buckets ADD CONSTRAINT fk_bucket_credential FOREIGN KEY (credential_id) REFERENCES bucket_credentials(id);