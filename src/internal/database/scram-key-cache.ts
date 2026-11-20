import { createHash, createHmac } from 'node:crypto'
import { createLruCache, SCRAM_KEY_CACHE_NAME } from '@internal/cache'
import pgCryptoUtils from 'pg/lib/crypto/utils'
import { getConfig } from '../../config'

const utils = pgCryptoUtils
let originals: typeof utils | undefined

export function installScramKeyCache() {
  if (originals) return
  originals = { ...utils }
  const { deriveKey } = originals

  const { isMultitenant, tenantPoolCacheMaxEntries } = getConfig()
  const cache = createLruCache<string, Promise<ArrayBuffer>>(SCRAM_KEY_CACHE_NAME, {
    max: isMultitenant ? tenantPoolCacheMaxEntries : 1,
    ttl: 60 * 60 * 1000,
  })

  utils.deriveKey = (password, salt, iterations) => {
    const saltBuffer = Buffer.isBuffer(salt)
      ? salt
      : Buffer.from(salt.buffer, salt.byteOffset, salt.byteLength)
    const key = createHash('sha256')
      .update(`${iterations}:${saltBuffer.toString('base64')}:`)
      .update(password)
      .digest('base64')
    let derived = cache.get(key)
    if (!derived) {
      const pending = deriveKey(password, salt, iterations)
      derived = pending
      pending.catch(() => {
        if (cache.peek(key) === pending) cache.delete(key)
      })
      cache.set(key, pending)
    }
    return derived
  }
  utils.hmacSha256 = async (key, msg) => createHmac('sha256', key).update(msg).digest()
  utils.sha256 = async (data) =>
    createHash('sha256')
      .update(ArrayBuffer.isView(data) ? data : new Uint8Array(data))
      .digest()
}

export function uninstallScramKeyCache() {
  if (!originals) return
  Object.assign(utils, originals)
  originals = undefined
}
