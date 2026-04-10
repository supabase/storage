import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'

const originalAuthEncryptionKey = process.env.AUTH_ENCRYPTION_KEY
const originalEncryptionKey = process.env.ENCRYPTION_KEY
const testEncryptionKey = 'pässwörd🔐'
const plaintext = 'payload-with-unicode-åß∂ƒ 🚀'
const legacyCiphertext =
  'U2FsdGVkX19JcSpAtQJU9fvPXdI8x6Z+4ypCCnuXdgd/Zs58/g+VpYtZbrJxC/IXXfEQuzgK4qamUe5rFiuxsA=='
const deterministicCiphertext =
  'U2FsdGVkX18AAQIDBAUGBygEQu5lvcWZgoqOtz6uMHKNaYgKr4hzXYxDM0EVHrks1kCp7vbFjcIAbNivFk4DzQ=='

process.env.AUTH_ENCRYPTION_KEY = testEncryptionKey
process.env.ENCRYPTION_KEY = testEncryptionKey

let encrypt: typeof import('../../src/internal/auth/crypto').encrypt
let decrypt: typeof import('../../src/internal/auth/crypto').decrypt

beforeAll(async () => {
  // The crypto module reads AUTH_ENCRYPTION_KEY / ENCRYPTION_KEY at module
  // load time. If another test in the same worker (e.g. jwt.test.ts) pulled
  // in @internal/auth transitively, the module is already cached with a
  // different key. Reset the module registry so our fresh import picks up
  // the keys we set above.
  vi.resetModules()
  ;({ encrypt, decrypt } = await import('../../src/internal/auth/crypto'))
})

afterAll(() => {
  if (originalAuthEncryptionKey === undefined) {
    delete process.env.AUTH_ENCRYPTION_KEY
  } else {
    process.env.AUTH_ENCRYPTION_KEY = originalAuthEncryptionKey
  }
  if (originalEncryptionKey === undefined) {
    delete process.env.ENCRYPTION_KEY
  } else {
    process.env.ENCRYPTION_KEY = originalEncryptionKey
  }
})

describe('auth crypto', () => {
  test('decrypts legacy CryptoJS ciphertext', () => {
    expect(decrypt(legacyCiphertext)).toBe(plaintext)
  })

  test('decrypts fixed-salt CryptoJS ciphertext', () => {
    expect(decrypt(deterministicCiphertext)).toBe(plaintext)
  })

  test('Node encrypt/decrypt roundtrip is stable', () => {
    expect(decrypt(encrypt(plaintext))).toBe(plaintext)
  })
})
