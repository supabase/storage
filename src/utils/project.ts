import * as CryptoJS from 'crypto-js'

import { createClient } from '@supabase/supabase-js'
import { getConfig } from '../utils/config'

const { supabaseReadOnlyUrl, supabaseApiKey, supabaseEncryptionKey } = getConfig()

interface JwtSecretAndServiceApiKeys {
  anonKey: string
  jwtSecret: string
  serviceKey: string
}

const jwtSecretAndServiceApiKeysCache: {
  [tenantId: string]: JwtSecretAndServiceApiKeys
} = {}

async function getJwtSecretAndServiceApiKeys(
  tenantId: string
): Promise<JwtSecretAndServiceApiKeys> {
  if (jwtSecretAndServiceApiKeysCache[tenantId]) {
    return jwtSecretAndServiceApiKeysCache[tenantId]
  }
  if (!supabaseReadOnlyUrl) {
    throw new Error('SUPABASE_READ_ONLY_URL environment variable is not set')
  }
  if (!supabaseApiKey) {
    throw new Error('SUPABASE_API_KEY environment variable is not set')
  }
  const supabase = createClient(supabaseReadOnlyUrl, supabaseApiKey)
  const {
    data: {
      jwt_secret_encrypted,
      services: [{ service_api_keys }],
    },
  } = await supabase
    .from('projects')
    .select('jwt_secret_encrypted, services(service_api_keys(api_key_encrypted, tags))')
    .eq('ref', tenantId)
    .single()
  if (!supabaseEncryptionKey) {
    throw new Error('SUPABASE_ENCRYPTION_KEY environment variable is not set')
  }
  const anonKeyEncrypted = service_api_keys.filter(
    (key: { tags: string }) => key.tags === 'anon'
  )[0].api_key_encrypted
  const serviceKeyEncrypted = service_api_keys.filter(
    (key: { tags: string }) => key.tags === 'service_role'
  )[0].api_key_encrypted
  const jwtSecretAndServiceApiKeys = {
    anonKey: CryptoJS.AES.decrypt(anonKeyEncrypted, supabaseEncryptionKey).toString(
      CryptoJS.enc.Utf8
    ),
    jwtSecret: CryptoJS.AES.decrypt(jwt_secret_encrypted, supabaseEncryptionKey).toString(
      CryptoJS.enc.Utf8
    ),
    serviceKey: CryptoJS.AES.decrypt(serviceKeyEncrypted, supabaseEncryptionKey).toString(
      CryptoJS.enc.Utf8
    ),
  }
  jwtSecretAndServiceApiKeysCache[tenantId] = jwtSecretAndServiceApiKeys
  return jwtSecretAndServiceApiKeys
}

export async function getAnonKey(tenantId: string): Promise<string> {
  const { anonKey } = await getJwtSecretAndServiceApiKeys(tenantId)
  return anonKey
}

export async function getServiceKey(tenantId: string): Promise<string> {
  const { serviceKey } = await getJwtSecretAndServiceApiKeys(tenantId)
  return serviceKey
}

export async function getJwtSecret(tenantId: string): Promise<string> {
  const { jwtSecret } = await getJwtSecretAndServiceApiKeys(tenantId)
  return jwtSecret
}
