

ALTER TABLE tenants_s3_credentials ADD COLUMN scopes json NOT NULL DEFAULT '{}';
