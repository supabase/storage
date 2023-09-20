# Supabase Storage Middleware

[![Coverage Status](https://coveralls.io/repos/github/supabase/storage-api/badge.svg?branch=master)](https://coveralls.io/github/supabase/storage-api?branch=master)

A scalable, light-weight object storage service.

> Read [this post](https://supabase.io/blog/2021/03/30/supabase-storage) on why we decided to build a new object storage service.

- Uses Postgres as its datastore for storing metadata
- Authorization rules are written as Postgres Row Level Security policies
- Integrates with S3 as the storage backend (with more in the pipeline!)
- Extremely lightweight and performant

![Architecture](./static/architecture.png?raw=true 'Architecture')

## Documentation

- [API Docs](https://supabase.github.io/storage-api/#/)
- [Storage Guides](https://supabase.io/docs/guides/storage)
- [Client library](https://supabase.io/docs/reference/javascript/storage-createbucket)

## Development

- Copy `.env.sample` to `.env` file.
- Copy `.env.test.sample` to `.env.test`.
- Change `GLOBAL_S3_BUCKET` and `REGION` to the name and region of a S3 bucket.
  - If you just want to run the tests and not develop locally, you can skip this step because S3 calls are mocked in our tests.
- [Set up your AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Your user must have permissions to `s3:PutObject, s3:GetObject, s3:DeleteObject` in the bucket you have chosen.

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

