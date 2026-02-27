import AES from 'crypto-js/aes'
import Utf8 from 'crypto-js/enc-utf8'

const originalAuthEncryptionKey = process.env.AUTH_ENCRYPTION_KEY
const originalEncryptionKey = process.env.ENCRYPTION_KEY
const testEncryptionKey = 'pässwörd🔐'
const plaintext = 'payload-with-unicode-åß∂ƒ 🚀'

process.env.AUTH_ENCRYPTION_KEY = testEncryptionKey
process.env.ENCRYPTION_KEY = testEncryptionKey

let encrypt: typeof import('../internal/auth/crypto').encrypt
let decrypt: typeof import('../internal/auth/crypto').decrypt

beforeAll(async () => {
  ;({ encrypt, decrypt } = await import('../internal/auth/crypto'))
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
  test('CryptoJS encrypt and Node decrypt are compatible', () => {
    const legacyCiphertext = AES.encrypt(plaintext, testEncryptionKey).toString()

    expect(decrypt(legacyCiphertext)).toBe(plaintext)
  })

  test('Node encrypt and CryptoJS decrypt are compatible', () => {
    const nodeCiphertext = encrypt(plaintext)

    expect(AES.decrypt(nodeCiphertext, testEncryptionKey).toString(Utf8)).toBe(plaintext)
  })

  test('Node encrypt/decrypt roundtrip is stable', () => {
    expect(decrypt(encrypt(plaintext))).toBe(plaintext)
  })
})
