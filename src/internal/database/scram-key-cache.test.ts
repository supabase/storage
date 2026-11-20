import crypto, { createHash, createHmac, pbkdf2Sync } from 'node:crypto'
import { SCRAM_KEY_CACHE_NAME } from '@internal/cache'
import { installScramKeyCache, uninstallScramKeyCache } from '@internal/database/scram-key-cache'
import * as metrics from '@internal/monitoring/metrics'
import sasl from 'pg/lib/crypto/sasl'
import utils from 'pg/lib/crypto/utils'
import { type Mock, vi } from 'vitest'
import { getConfig } from '../../config'
import { getCacheRequestCalls } from '../../test/utils/cache-metrics'

const CLIENT_NONCE = 'rOprNGfwEbeRWgbNEkqO'
const SERVER_FIRST =
  'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
const CLIENT_FIRST_BARE = `n=*,r=${CLIENT_NONCE}`
const CLIENT_FINAL_NO_PROOF = 'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
const AUTH_MESSAGE = `${CLIENT_FIRST_BARE},${SERVER_FIRST},${CLIENT_FINAL_NO_PROOF}`

// pg sends n=* instead of the RFC's n=user, so the RFC 7677 proof does not apply;
// derive the expected values independently from RFC 5802 with node:crypto.
const hmac = (key: Buffer, msg: string) => createHmac('sha256', key).update(msg).digest()
const salted = pbkdf2Sync(
  'pencil',
  Buffer.from('W22ZaJ0SNY7soEsUEjb6gQ==', 'base64'),
  4096,
  32,
  'sha256'
)
const clientKey = hmac(salted, 'Client Key')
const clientSignature = hmac(createHash('sha256').update(clientKey).digest(), AUTH_MESSAGE)
const proof = Buffer.from(clientKey.map((b, i) => b ^ clientSignature[i])).toString('base64')
const CLIENT_FINAL = `${CLIENT_FINAL_NO_PROOF},p=${proof}`
const SERVER_FINAL = `v=${hmac(hmac(salted, 'Server Key'), AUTH_MESSAGE).toString('base64')}`

async function runExchange() {
  const session = {
    mechanism: 'SCRAM-SHA-256',
    clientNonce: CLIENT_NONCE,
    response: `n,,n=*,r=${CLIENT_NONCE}`,
    message: 'SASLInitialResponse',
  }
  await sasl.continueSession(session, 'pencil', SERVER_FIRST)
  expect(session.response).toBe(CLIENT_FINAL)
  sasl.finalizeSession(session, SERVER_FINAL)
}

describe('scram key cache', () => {
  const original = { ...utils }

  afterEach(() => {
    uninstallScramKeyCache()
    Object.assign(utils, original)
    vi.restoreAllMocks()
    vi.unstubAllEnvs()
    getConfig({ reload: true })
  })

  test('pg crypto utils keep the signatures the cache patches', () => {
    expect(utils.deriveKey.length).toBe(3)
    expect(utils.hmacSha256.length).toBe(2)
    expect(utils.sha256.length).toBe(1)
  })

  test('SCRAM vector passes before install', async () => {
    await runExchange()
  })

  test('SCRAM vector passes after install and install is idempotent', async () => {
    installScramKeyCache()
    const patched = utils.deriveKey
    installScramKeyCache()
    expect(utils.deriveKey).toBe(patched)
    expect(patched).not.toBe(original.deriveKey)
    await runExchange()
    await runExchange()
  })

  test('uninstall restores originals', () => {
    installScramKeyCache()
    uninstallScramKeyCache()
    expect(utils.deriveKey).toBe(original.deriveKey)
    expect(utils.hmacSha256).toBe(original.hmacSha256)
    expect(utils.sha256).toBe(original.sha256)
  })

  test('sasl.continueSession derives once and uses no webcrypto sign/digest', async () => {
    const subtle = (crypto.webcrypto ?? globalThis.crypto).subtle
    installScramKeyCache()
    const deriveBits = vi.spyOn(subtle, 'deriveBits')
    const sign = vi.spyOn(subtle, 'sign')
    const digest = vi.spyOn(subtle, 'digest')
    await runExchange()
    await runExchange()
    expect(deriveBits).toHaveBeenCalledTimes(1)
    expect(sign).not.toHaveBeenCalled()
    expect(digest).not.toHaveBeenCalled()
  })

  describe('deriveKey memoization', () => {
    const salt = Buffer.from('salty')
    let spy: Mock<typeof utils.deriveKey>

    beforeEach(() => {
      spy = vi.fn<typeof utils.deriveKey>(
        async (password: string) => new TextEncoder().encode(password).buffer
      )
      utils.deriveKey = spy
      installScramKeyCache()
    })

    test('reuses the result for identical inputs', async () => {
      const a = await utils.deriveKey('pw', salt, 4096)
      const b = await utils.deriveKey('pw', new Uint8Array(Buffer.from('salty')), 4096)
      expect(b).toBe(a)
      expect(spy).toHaveBeenCalledTimes(1)
    })

    test('derives again when password, salt or iterations differ', async () => {
      await utils.deriveKey('pw', salt, 4096)
      await utils.deriveKey('pw2', salt, 4096)
      await utils.deriveKey('pw', Buffer.from('other'), 4096)
      await utils.deriveKey('pw', salt, 4097)
      expect(spy).toHaveBeenCalledTimes(4)
    })

    test('concurrent calls share one derivation', async () => {
      await Promise.all([
        utils.deriveKey('pw', salt, 4096),
        utils.deriveKey('pw', salt, 4096),
        utils.deriveKey('pw', salt, 4096),
      ])
      expect(spy).toHaveBeenCalledTimes(1)
    })

    test('does not cache a rejected derivation', async () => {
      spy.mockRejectedValueOnce(new Error('boom'))
      await expect(utils.deriveKey('pw', salt, 4096)).rejects.toThrow('boom')
      await utils.deriveKey('pw', salt, 4096)
      expect(spy).toHaveBeenCalledTimes(2)
    })

    test('a late rejection does not delete a replacement entry', async () => {
      uninstallScramKeyCache()
      vi.stubEnv('MULTI_TENANT', 'false')
      getConfig({ reload: true })
      utils.deriveKey = spy
      installScramKeyCache()

      let rejectFirst!: (err: Error) => void
      spy.mockImplementationOnce(
        () =>
          new Promise<ArrayBuffer>((_, reject) => {
            rejectFirst = reject
          })
      )
      const first = utils.deriveKey('a', salt, 1)
      first.catch(() => {})
      await utils.deriveKey('b', salt, 1)
      await utils.deriveKey('a', salt, 1)
      expect(spy).toHaveBeenCalledTimes(3)

      rejectFirst(new Error('late'))
      await expect(first).rejects.toThrow('late')
      await utils.deriveKey('a', salt, 1)
      expect(spy).toHaveBeenCalledTimes(3)
    })

    test('records hits and misses under the scram_key cache name', async () => {
      const record = vi.spyOn(metrics, 'recordCacheRequest')
      await utils.deriveKey('pw', salt, 4096)
      await utils.deriveKey('pw', salt, 4096)
      expect(getCacheRequestCalls(record, SCRAM_KEY_CACHE_NAME)).toEqual([
        [SCRAM_KEY_CACHE_NAME, 'miss'],
        [SCRAM_KEY_CACHE_NAME, 'hit'],
      ])
    })
  })
})
