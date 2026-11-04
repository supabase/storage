import {
  assertGuardedHeadersSigned,
  isGuardedAmzHeaderName,
  isGuardedCreateBucketHeaderName,
} from './signed-header-guard'

describe('assertGuardedHeadersSigned with isGuardedAmzHeaderName for CopyObject', () => {
  it('allows a signed x-amz-copy-source header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-copy-source': '/bucket/key' },
        new Set(['x-amz-copy-source']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('rejects an unsigned x-amz-copy-source header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-copy-source': '/bucket/key' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).toThrow(/x-amz-copy-source/)
  })

  it.each([
    'x-amz-copy-source-range',
    'x-amz-copy-source-if-match',
    'x-amz-copy-source-if-none-match',
    'x-amz-copy-source-if-modified-since',
    'x-amz-copy-source-if-unmodified-since',
  ])('rejects an unsigned %s header', (name) => {
    expect(() =>
      assertGuardedHeadersSigned({ [name]: 'value' }, new Set(['host']), isGuardedAmzHeaderName)
    ).toThrow(new RegExp(name))
  })

  it('rejects an unsigned x-amz-metadata-directive header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-metadata-directive': 'REPLACE' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).toThrow(/x-amz-metadata-directive/)
  })

  it('allows a signed x-amz-metadata-directive header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-metadata-directive': 'REPLACE' },
        new Set(['x-amz-metadata-directive']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('ignores a guarded header that is absent from the request', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-copy-source': undefined },
        new Set(),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('rejects a guarded header when no headers were signed at all', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-copy-source': '/bucket/key' },
        undefined,
        isGuardedAmzHeaderName
      )
    ).toThrow()
  })
})

describe('assertGuardedHeadersSigned with isGuardedCreateBucketHeaderName', () => {
  it('allows a request with no x-amz-acl header present', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { host: 'example.com' },
        new Set(['host']),
        isGuardedCreateBucketHeaderName
      )
    ).not.toThrow()
  })

  it('rejects an unsigned x-amz-acl header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-acl': 'public-read' },
        new Set(['host']),
        isGuardedCreateBucketHeaderName
      )
    ).toThrow(/x-amz-acl/)
  })

  it('allows a signed x-amz-acl header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-acl': 'public-read' },
        new Set(['x-amz-acl']),
        isGuardedCreateBucketHeaderName
      )
    ).not.toThrow()
  })

  it('ignores headers other than x-amz-acl', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-copy-source': '/bucket/key' },
        new Set(['host']),
        isGuardedCreateBucketHeaderName
      )
    ).not.toThrow()
  })
})

describe('assertGuardedHeadersSigned with isGuardedAmzHeaderName', () => {
  it('allows a request with no x-amz- headers present', () => {
    expect(() =>
      assertGuardedHeadersSigned({ host: 'example.com' }, new Set(['host']), isGuardedAmzHeaderName)
    ).not.toThrow()
  })

  it('rejects an unsigned x-amz-meta- custom metadata header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-meta-foo': 'bar' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).toThrow(/x-amz-meta-foo/)
  })

  it('allows a signed x-amz-meta- custom metadata header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-meta-foo': 'bar' },
        new Set(['x-amz-meta-foo']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('rejects an unsigned x-amz-decoded-content-length header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-decoded-content-length': '1024' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).toThrow(/x-amz-decoded-content-length/)
  })

  it('allows a signed x-amz-decoded-content-length header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-decoded-content-length': '1024' },
        new Set(['x-amz-decoded-content-length']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('rejects an unsigned, previously-unseen x-amz- header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-some-header-added-after-this-test-was-written': 'value' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).toThrow(/x-amz-some-header-added-after-this-test-was-written/)
  })

  it('allows a signed, previously-unseen x-amz- header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-some-header-added-after-this-test-was-written': 'value' },
        new Set(['x-amz-some-header-added-after-this-test-was-written']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('allows an unsigned x-amz-content-sha256 header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        {
          'x-amz-content-sha256':
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
        },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('allows an unsigned x-amz-cf-id header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'x-amz-cf-id': 'some-cloudfront-request-id' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it('allows an unsigned cache-control header', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'cache-control': 'max-age=2009' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })

  it.each(['content-type', 'content-encoding'])(
    'ignores an unsigned %s header (not x-amz-*-prefixed)',
    (name) => {
      expect(() =>
        assertGuardedHeadersSigned({ [name]: 'value' }, new Set(['host']), isGuardedAmzHeaderName)
      ).not.toThrow()
    }
  )

  it('ignores unrelated unsigned headers', () => {
    expect(() =>
      assertGuardedHeadersSigned(
        { 'user-agent': 'aws-sdk-js/3.0' },
        new Set(['host']),
        isGuardedAmzHeaderName
      )
    ).not.toThrow()
  })
})
