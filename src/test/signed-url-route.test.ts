import { doesSignedTokenMatchRequestPath } from '../http/routes/signed-url'
import { encodePathPreservingSeparatorsForTest } from './utils/path-encoding'

describe('signed URL route path verification', () => {
  test('matches canonical encoded object path for object signed route', () => {
    const signedObjectPath = 'bucket2/public/일이삼-🙂-q?foo=1&bar=%25+plus;semi:colon,#frag.png'
    const rawPath = `/object/sign/${encodePathPreservingSeparatorsForTest(signedObjectPath)}?token=jwt`

    expect(doesSignedTokenMatchRequestPath(rawPath, '/object/sign', signedObjectPath)).toBe(true)
  })

  test('matches canonical encoded object path for render signed route', () => {
    const signedObjectPath = 'bucket2/authenticated/casestudy.png'
    const rawPath = `/render/image/sign/${encodePathPreservingSeparatorsForTest(signedObjectPath)}?token=jwt`

    expect(doesSignedTokenMatchRequestPath(rawPath, '/render/image/sign', signedObjectPath)).toBe(
      true
    )
  })

  test('matches canonical encoded object path for upload signed route', () => {
    const signedObjectPath = 'bucket2/public/일이삼-🙂-q?foo=1&bar=%25+plus;semi:colon,#frag.png'
    const rawPath = `/object/upload/sign/${encodePathPreservingSeparatorsForTest(signedObjectPath)}?token=jwt`

    expect(doesSignedTokenMatchRequestPath(rawPath, '/object/upload/sign', signedObjectPath)).toBe(
      true
    )
  })

  test('rejects double-encoded request paths', () => {
    const signedObjectPath = 'bucket2/public/일이삼.txt'
    const encodedPath = encodePathPreservingSeparatorsForTest(signedObjectPath)
    const doubleEncodedPath = encodedPath.replaceAll('%', '%25')
    const rawPath = `/object/sign/${doubleEncodedPath}?token=jwt`

    expect(doesSignedTokenMatchRequestPath(rawPath, '/object/sign', signedObjectPath)).toBe(false)
  })

  test('rejects decoded raw unicode path', () => {
    const signedObjectPath = 'bucket2/public/일이삼.txt'
    const rawPath = `/object/sign/${signedObjectPath}?token=jwt`

    expect(doesSignedTokenMatchRequestPath(rawPath, '/object/sign', signedObjectPath)).toBe(false)
  })

  test('returns false for missing raw url', () => {
    expect(
      doesSignedTokenMatchRequestPath(undefined, '/object/sign', 'bucket2/public/sadcat-upload.png')
    ).toBe(false)
  })
})
