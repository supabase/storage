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
