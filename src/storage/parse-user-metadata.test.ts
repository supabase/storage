import { parseUserMetadata } from './uploader'

const encode = (value: string) => Buffer.from(value, 'utf8').toString('base64')

describe('parseUserMetadata', () => {
  it('parses a base64 encoded JSON object', () => {
    expect(parseUserMetadata(encode(JSON.stringify({ owner: 'alice', size: 3 })))).toEqual({
      owner: 'alice',
      size: 3,
    })
  })

  it('keeps nested values', () => {
    const metadata = { tags: ['a', 'b'], nested: { ok: true } }
    expect(parseUserMetadata(encode(JSON.stringify(metadata)))).toEqual(metadata)
  })

  it.each([
    ['null', 'null'],
    ['an array', '[1,2,3]'],
    ['a number', '42'],
    ['a string', '"hello"'],
    ['a boolean', 'true'],
  ])('returns undefined when the JSON is %s', (_label, json) => {
    expect(parseUserMetadata(encode(json))).toBeUndefined()
  })

  it('returns undefined for invalid JSON', () => {
    expect(parseUserMetadata(encode('{not json'))).toBeUndefined()
  })
})
