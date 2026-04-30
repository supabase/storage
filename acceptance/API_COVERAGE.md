# Acceptance API Coverage

The acceptance suite is black-box by design. Tests only use public HTTP, S3, TUS, and admin
surfaces so the same contracts can be run against the current TypeScript service or a future
Go/Rust rewrite.

## Core Coverage

These run in `smoke` / `core` profiles and are included in the default local CI `full` profile:

| Area                   | Covered APIs / behavior                                                                                                           |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Health                 | `/status`, `/version`                                                                                                             |
| REST buckets           | create, get, list/search, update, empty, delete                                                                                   |
| REST objects           | upload, update, authenticated read/head/info, public read/head/info, delete                                                       |
| REST object operations | list-v1, list-v2, signed URL, batch signed URLs, signed upload URL, copy, move, bulk delete                                       |
| S3 buckets             | CreateBucket, HeadBucket, ListBuckets, GetBucketLocation, GetBucketVersioning, DeleteBucket                                       |
| S3 objects             | PutObject, HeadObject, GetObject, Range GetObject, CopyObject, DeleteObject, DeleteObjects                                        |
| S3 listing             | ListObjectsV2, ListObjects V1 with delimiter/common prefixes                                                                      |
| S3 multipart           | CreateMultipartUpload, UploadPart, ListParts, CompleteMultipartUpload, UploadPartCopy, AbortMultipartUpload, ListMultipartUploads |
| TUS                    | OPTIONS, POST create, HEAD offset, PATCH resume, DELETE termination, full upload through `tus-js-client`, signed TUS upload       |

## Wire Profile Coverage

These run in the `wire` profile in addition to smoke coverage:

| Area       | Covered APIs / behavior                                                 |
| ---------- | ----------------------------------------------------------------------- |
| Wire/SigV4 | raw `aws-chunked` PutObject and UploadPart, trailer-signature rejection |

## Capability-Gated Coverage

These require target-specific capabilities. They run when the capability flag is enabled and the
selected profile includes the spec:

| Capability | Enable with                                                                             | Covered APIs / behavior                                                                                                                                                                              |
| ---------- | --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Admin      | `ACCEPTANCE_ENABLE_ADMIN=true` plus admin URL/API key                                   | admin status, API key protection, tenant reads, tenant migration state, metrics config, queue/migration validation, JWKS validation/status, orphan scan validation, S3 credential create/list/delete |
| CDN        | `ACCEPTANCE_ENABLE_CDN=true`                                                            | `/cdn/:bucket/*` cache purge                                                                                                                                                                         |
| Render     | `ACCEPTANCE_ENABLE_RENDER=true`                                                         | public, authenticated, and signed image transformation routes                                                                                                                                        |
| RLS        | `ACCEPTANCE_ENABLE_RLS_SETUP=true` plus anon/authenticated keys and RLS resource config | authenticated allow and anon deny for read/write on configured policies                                                                                                                              |
| Path edges | `ACCEPTANCE_ENABLE_PATH_EDGES=true`                                                     | list-v2 preservation for object names with empty path segments, only on targets whose blob backend accepts those names                                                                               |
| Vector     | `ACCEPTANCE_ENABLE_VECTOR=true`                                                         | vector bucket, index, put/get/list/query/delete lifecycle                                                                                                                                            |
| Iceberg    | `ACCEPTANCE_ENABLE_ICEBERG=true`                                                        | analytics bucket, catalog config, namespace, table create/list/load/head/drop                                                                                                                        |

## Intentionally Gated

Remote runs should set `ACCEPTANCE_TARGET=remote`. Destructive tests are blocked on remote targets
unless `ACCEPTANCE_ALLOW_DESTRUCTIVE=true` is set. Admin, CDN, render, vector, Iceberg, and RLS
tests are capability-gated because they depend on tenant features or credentials that are not
universally available.
