# Supabase Storage Engine

[![Coverage Status](https://coveralls.io/repos/github/supabase/storage-api/badge.svg?branch=master)](https://coveralls.io/github/supabase/storage-api?branch=master)

A scalable, light-weight object storage service.

> Read [this post](https://supabase.io/blog/2021/03/30/supabase-storage) on why we decided to build a new object storage service.

- Multi-protocol support (HTTP, TUS, S3)
- Uses Postgres as its datastore for storing metadata
- Authorization rules are written as Postgres Row Level Security policies
- Integrates with S3 Compatible Storages
- Extremely lightweight and performant


**Supported Protocols**

- [x] HTTP/REST
- [x] TUS Resumable Upload
- [x] S3 Compatible API

![Architecture](./static/architecture.png?raw=true 'Architecture')

## Documentation

- [OpenAPI Spec](https://supabase.github.io/storage)
- [Storage Guides](https://supabase.io/docs/guides/storage)
- [Client library](https://supabase.io/docs/reference/javascript/storage-createbucket)

## Development

- Copy `.env.sample` to `.env` file.
- Copy `.env.test.sample` to `.env.test`.

```bash
cp .env.sample .env && cp .env.test.sample .env.test
````

**Your root directory should now have both `.env` and `.env.test` files.**

- Then run the following:

```bash
# this sets up a postgres database and postgrest locally via docker
npm run infra:restart
# Start the storage server
npm run dev
```

The server should now be running at http://localhost:5000/

The following request should insert and return the list of buckets.

```bash
# insert a bucket named avatars
curl --location --request POST 'http://localhost:5000/bucket' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIiwiaWF0IjoxNjEzNTMxOTg1LCJleHAiOjE5MjkxMDc5ODV9.th84OKK0Iz8QchDyXZRrojmKSEZ-OuitQm_5DvLiSIc' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "avatars"
}'

# get buckets
curl --location --request GET 'http://localhost:5000/bucket' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIiwiaWF0IjoxNjEzNTMxOTg1LCJleHAiOjE5MjkxMDc5ODV9.th84OKK0Iz8QchDyXZRrojmKSEZ-OuitQm_5DvLiSIc'
```

### Testing

To perform your tests you can run the following command: `npm test`

### Running in context

Sometimes it is useful to test changes in the context of the entire stack (e.g. a project running via the Supabase cli). The `replace-existing-container.sh` script builds an image and replace an existing running container with it. All settings of the existing container are preserved (volumes, network, env, etc).

```bash
# Start supabase project - in root of supabase project "PROJECT-NAME"
supabase start

# In root of storage api repo
./scripts/replace-existing-container.sh supabase_storage_PROJECT-NAME
```
