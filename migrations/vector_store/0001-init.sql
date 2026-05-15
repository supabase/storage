-- Initial schema for the pgvector-backed vector store.
-- Creates the extension and the dedicated `storage_vectors` schema where the
-- pgvector adapter will materialise one table per (bucket, index) pair at
-- runtime via CREATE TABLE.
--
-- This migration only runs when VECTOR_BUCKET_PROVIDER=pgvector and
-- VECTOR_STORE_MIGRATIONS_ENABLED=true (gated by the migration runner).
--
-- The pgvector extension MUST live in a schema present in the runtime
-- connection's search_path (storage, public, extensions, …). We pin it to
-- `public` because it's guaranteed to exist on every Postgres install.

CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;

CREATE SCHEMA IF NOT EXISTS storage_vectors;
