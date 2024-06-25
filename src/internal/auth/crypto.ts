import AES from 'crypto-js/aes'
import Utf8 from 'crypto-js/enc-utf8'
import { getConfig } from '../../config'

const { encryptionKey } = getConfig()

/**
 * Decrypts a text with the configured encryption key via ENCRYPTION_KEY env
 * @param ciphertext
 */
export function decrypt(ciphertext: string): string {
  return AES.decrypt(ciphertext, encryptionKey).toString(Utf8)
}

/**
 * Encrypts a text with the configured encryption key via ENCRYPTION_KEY env
 * @param plaintext
 */
export function encrypt(plaintext: string): string {
  return AES.encrypt(plaintext, encryptionKey).toString()
}
