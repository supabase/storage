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
})
