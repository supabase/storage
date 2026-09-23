import { performance } from 'node:perf_hooks'
import { assertPolicyConditionsSatisfied, Policy } from './policy'

describe('POST policy conditions', () => {
  it('rejects a Content-Type list when any value violates a starts-with condition', () => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, {
        'content-type': 'image/png,text/plain',
      })
    ).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
      })
    )
  })

  it('accepts a Content-Type list when every value satisfies a starts-with condition', () => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, {
        'content-type': 'image/png,image/gif',
      })
    ).not.toThrow()
  })

  it.each([
    'image/png, image/gif',
    'image/png,',
  ])('accepts Content-Type list %j like S3: members are trimmed, trailing empty members ignored', (contentType) => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, { 'content-type': contentType })
    ).not.toThrow()
  })

  it.each([
    'image/png,,image/gif',
    'image/png, ,image/gif',
    ',image/png',
    'image/png, ',
    'image/png , ',
    'image/png, ,',
    '',
    ' ',
  ])('rejects Content-Type list %j like S3: empty or whitespace-only members fail', (contentType) => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, { 'content-type': contentType })
    ).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
      })
    )
  })

  // S3 accepts a lone comma (zero members, vacuous match) and stores "," as the
  // Content-Type. Deliberate deviation: keep the starts-with guarantee instead.
  it('rejects a Content-Type of only commas', () => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }

    expect(() => assertPolicyConditionsSatisfied(policy, { 'content-type': ',' })).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
      })
    )
  })

  it.each([
    'image/png,image/png',
    'image/png,',
    'image/png, image/png',
    ' image/png',
  ])('rejects Content-Type %j under an eq condition like S3: eq never splits lists', (contentType) => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['eq', '$Content-Type', 'image/png']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, { 'content-type': contentType })
    ).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
      })
    )
  })

  it('rejects a long invalid Content-Type list without blocking on trailing-comma matching', () => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$Content-Type', 'image/']],
    }
    const contentType = `image/png${','.repeat(80_000)}text/plain`

    const startedAt = performance.now()
    expect(() =>
      assertPolicyConditionsSatisfied(policy, {
        'content-type': contentType,
      })
    ).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
      })
    )

    expect(performance.now() - startedAt).toBeLessThan(500)
  })

  it('continues treating commas in other fields as ordinary string content', () => {
    const policy: Policy = {
      expiration: '2099-01-01T00:00:00Z',
      conditions: [['starts-with', '$key', 'folder,a/']],
    }

    expect(() =>
      assertPolicyConditionsSatisfied(policy, {
        key: 'folder,a/object.txt',
      })
    ).not.toThrow()
  })
})
