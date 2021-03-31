# Supabase storage middleware

## Development

- Copy `.env.sample` to `.env` file.
- Change `GLOBAL_S3_BUCKET` and `REGION` to the name and region of a S3 bucket. If you just want to run the tests and not develop locally, you can skip this step because S3 calls are mocked in our tests.
- Copy `.env.test.sample` to `.env.test`
- Set up your AWS credentials. Your user must have permissions to `s3:PutObject , s3:GetObject, s3:DeleteObject` in the bucket you have chosen.
- Then run the following

```bash
npm run restart:db
npm run dev
```

## Testing

Run `npm test`
