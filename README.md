# Supabase Storage Middleware

A scalable, light-weight object storage service. Read [this post](https://supabase.io/blog/2021/03/30/supabase-storage) on why we decided to build a new object storage service.

- Uses Postgres as it's datastore for storing metadata
- Authorization rules are written as Postgres Row Level Security policies.
- Integrates with S3 as the storage backend (with more in the pipeline!)

![Architecture](./static/architecture.png?raw=true 'Architecture')

## Documentation

- [Storage Guides](https://supabase.io/docs/guides/storage)
- [Client library](https://supabase.io/docs/reference/javascript/storage-createbucket)

## Development

- Copy `.env.sample` to `.env` file.
- Change `GLOBAL_S3_BUCKET` and `REGION` to the name and region of a S3 bucket. If you just want to run the tests and not develop locally, you can skip this step because S3 calls are mocked in our tests.
- Copy `.env.test.sample` to `.env.test`. Your root directory should now have both `.env` and `.env.test` files.
- [Set up your AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Your user must have permissions to `s3:PutObject , s3:GetObject, s3:DeleteObject` in the bucket you have chosen.
- Then run the following

```bash
# this sets up a postgres database and postgrest locally via docker
npm run restart:db
# Start the storage server
npm run dev
```

## Testing

Run `npm test`
