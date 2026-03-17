import { SignatureV4 } from '../storage/protocols/s3/signature-v4'

describe('SignatureV4.parseAuthorizationHeader', () => {
  const authorization =
    'AWS4-HMAC-SHA256 Credential=test-access/20260407/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=abc123'

  it('rejects duplicate authorization headers', () => {
    expect(() =>
      SignatureV4.parseAuthorizationHeader({
        authorization: [authorization, authorization],
        'x-amz-date': '20260407T120000Z',
      })
    ).toThrow('Multiple authorization headers are not supported')
  })

  it('rejects duplicate x-amz-date headers', () => {
    expect(() =>
      SignatureV4.parseAuthorizationHeader({
        authorization,
        'x-amz-date': ['20260407T120000Z', '20260407T120001Z'],
      })
    ).toThrow('Multiple x-amz-date headers are not supported')
  })
})
