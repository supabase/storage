import { validateVectorKey } from './limits'

describe('validateVectorKey', () => {
  test('accepts 1,024 Unicode characters even when UTF-16 uses more code units', () => {
    expect(() => validateVectorKey('😀'.repeat(1024), 'key')).not.toThrow()
  })

  test('rejects a key with more than 1,024 Unicode characters', () => {
    expect(() => validateVectorKey('😀'.repeat(1025), 'key')).toThrow(
      'key must be between 1 and 1024 characters'
    )
  })

  test('accepts 1,024 ASCII characters', () => {
    expect(() => validateVectorKey('a'.repeat(1024), 'key')).not.toThrow()
  })

  test('rejects an empty key', () => {
    expect(() => validateVectorKey('', 'key')).toThrow('key must be between 1 and 1024 characters')
  })

  test('rejects 1,025 ASCII characters', () => {
    expect(() => validateVectorKey('a'.repeat(1025), 'key')).toThrow(
      'key must be between 1 and 1024 characters'
    )
  })

  test('rejects 1,025 Unicode characters that fit in 2,048 UTF-16 code units', () => {
    expect(() => validateVectorKey('aa' + '😀'.repeat(1023), 'key')).toThrow(
      'key must be between 1 and 1024 characters'
    )
  })
})
