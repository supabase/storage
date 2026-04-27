# Acceptance Tests

This suite verifies Supabase Storage as a black-box service. It is intentionally separate from
`src/test`: acceptance tests must talk to a running target through HTTP, S3, TUS, or admin
endpoints only. Do not import `src/app`, use `app.inject`, or call storage/database classes here.
See [`API_COVERAGE.md`](./API_COVERAGE.md) for the API/feature coverage inventory.

## Profiles

- `smoke` - fast target sanity plus REST and S3 lifecycle checks.
- `core` - smoke plus broader protocol behavior such as TUS and multipart.
- `wire` - smoke plus raw HTTP/SigV4 cases such as `aws-chunked` bodies and trailer negatives.
- `full` - all acceptance tests allowed by the configured target and capability gates.

## Run Against An Existing Target

The runner loads `.env.acceptance`. To use a different acceptance env file, set
`ACCEPTANCE_ENV_FILE`.

```bash
ACCEPTANCE_BASE_URL=http://127.0.0.1:5000 \
ACCEPTANCE_S3_ENDPOINT=http://127.0.0.1:5000/s3 \
ACCEPTANCE_SERVICE_KEY="..." \
ACCEPTANCE_S3_ACCESS_KEY_ID="..." \
ACCEPTANCE_S3_SECRET_ACCESS_KEY="..." \
npm run acceptance:run -- --profile smoke
```

For a hosted target with a path prefix, set `ACCEPTANCE_BASE_URL` to the public storage base,
for example `https://project.supabase.co/storage/v1`. Relative URLs returned by the API are
resolved against that base so prefix behavior is covered. Set `ACCEPTANCE_TARGET=remote` for
hosted targets; destructive tests then require `ACCEPTANCE_ALLOW_DESTRUCTIVE=true`.

For local targets on a non-default port, set `ACCEPTANCE_BASE_URL` explicitly.

## Managed Local Run

```bash
cp .env.sample .env
cp .env.test.sample .env.test
cp .env.acceptance.sample .env.acceptance
npm run acceptance -- --profile smoke
```

This restarts local infra, seeds dummy data, starts the TypeScript server from `.env.test` plus
`.env`, waits for `/status`, runs the acceptance profile, and then stops the server. Set
`ACCEPTANCE_SKIP_INFRA=true` to reuse already-running local infra.

For local backend variants, put server/runtime changes in `.env` or `.env.test`. Keep
`.env.acceptance` limited to acceptance runner inputs such as target URLs, client credentials,
capability gates, and resource naming.

## GitHub Environments

The workflow dispatch `acceptance_environment` input uses `local` for the managed local run. Any
other option is treated as a GitHub Environment name and is used to populate `.env.acceptance` at
runtime. Store non-secret values such as `ACCEPTANCE_BASE_URL`, `ACCEPTANCE_REGION`, and capability
flags as environment variables, and store credentials such as `ACCEPTANCE_SERVICE_KEY` and S3
secrets as environment secrets.

## Useful Configuration

| Variable                          | Meaning                                                                      |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `ACCEPTANCE_BASE_URL`             | REST base URL. Defaults to `http://127.0.0.1:5000`.                          |
| `ACCEPTANCE_S3_ENDPOINT`          | S3 endpoint. Defaults to `$ACCEPTANCE_BASE_URL/s3`.                          |
| `ACCEPTANCE_TUS_ENDPOINT`         | TUS endpoint. Defaults to `$ACCEPTANCE_BASE_URL/upload/resumable`.           |
| `ACCEPTANCE_ADMIN_URL`            | Admin API base URL for admin tests.                                          |
| `ACCEPTANCE_SERVICE_KEY`          | Service role JWT for REST tests.                                             |
| `ACCEPTANCE_S3_ACCESS_KEY_ID`     | S3 protocol access key.                                                      |
| `ACCEPTANCE_S3_SECRET_ACCESS_KEY` | S3 protocol secret.                                                          |
| `ACCEPTANCE_REGION`               | S3 signing region. Defaults to `us-east-1`.                                  |
| `ACCEPTANCE_RESOURCE_PREFIX`      | Prefix for all resources created by this run.                                |
| `ACCEPTANCE_ENABLE_ADMIN`         | Enables admin route tests. Requires admin URL and API key.                   |
| `ACCEPTANCE_ENABLE_CDN`           | Enables CDN purge tests. Requires purge-cache support on the target tenant.  |
| `ACCEPTANCE_ENABLE_RENDER`        | Enables image transformation tests.                                          |
| `ACCEPTANCE_ENABLE_RLS_SETUP`     | Enables RLS tests; requires service, anon, authenticated keys and resources. |
| `ACCEPTANCE_ENABLE_VECTOR`        | Enables vector bucket API tests.                                             |
| `ACCEPTANCE_ENABLE_ICEBERG`       | Enables Iceberg catalog API tests.                                           |
| `ACCEPTANCE_ENABLE_PATH_EDGES`    | Enables object-name edge tests that require a backend accepting `//` names.  |
| `ACCEPTANCE_ENABLE_WIRE`          | Enables wire-level tests outside the `wire` / `full` profiles.               |
| `ACCEPTANCE_RLS_BUCKET`           | Bucket used by opt-in RLS tests. Defaults to local dummy `bucket2`.          |
| `ACCEPTANCE_RLS_READ_OBJECT`      | Existing object readable by authenticated role and denied to anon.           |
| `ACCEPTANCE_RLS_WRITE_PREFIX`     | Prefix where authenticated role may upload and anon may not.                 |
| `ACCEPTANCE_ALLOW_DESTRUCTIVE`    | Required for destructive tests when `ACCEPTANCE_TARGET=remote`.              |

## HTTPS And Wire Tests

The `wire` profile includes smoke coverage and uses a raw SigV4 client for `aws-chunked`
payloads. To verify proxy/TLS behavior, point `ACCEPTANCE_S3_ENDPOINT` at an HTTPS URL, for
example:

```bash
ACCEPTANCE_S3_ENDPOINT=https://storage.localhost/s3 \
ACCEPTANCE_TLS_REJECT_UNAUTHORIZED=false \
npm run acceptance:run -- --profile wire
```

`ACCEPTANCE_TLS_REJECT_UNAUTHORIZED=false` sets `NODE_TLS_REJECT_UNAUTHORIZED=0` in the runner
for local self-signed certificates. Do not use it for remote runs unless the target is explicitly
provisioned for that.
