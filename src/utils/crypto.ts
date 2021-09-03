import AES from 'crypto-js/aes'
import Utf8 from 'crypto-js/enc-utf8'
import { getConfig } from './config'

const { encryptionKey } = getConfig()

export function decrypt(ciphertext: string): string {
  return AES.decrypt(ciphertext, encryptionKey).toString(Utf8)
}

export function encrypt(plaintext: string): string {
  return AES.encrypt(plaintext, encryptionKey).toString()
}
