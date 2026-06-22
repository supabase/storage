import { logger } from '@internal/monitoring'
import fastDecodeURIComponent from 'fast-decode-uri-component'
import * as ipAddr from 'ip-address'
import { ConnectionOptions } from 'tls'

export function getSslSettings({
  connectionString,
  databaseSSLRootCert,
}: {
  connectionString: string
  databaseSSLRootCert: string | undefined
}): ConnectionOptions | undefined {
  if (!databaseSSLRootCert) return undefined

  try {
    // When connecting through PGBouncer, we connect through an IPv6 address rather than a hostname
    // When passing in the root CA for SSL, this will always fail, so we need to skip passing in the SSL root cert
    // in case the hostname is an IP address
    const url = new URL(connectionString)
    if (url.hostname && isIpAddress(url.hostname)) {
      return { ca: databaseSSLRootCert, rejectUnauthorized: false }
    }
  } catch (err) {
    // ignore to ensure this never breaks the connection in case of an invalid URL
    logger.warn(err, 'Failed to parse connection string')
  }

  return { ca: databaseSSLRootCert }
}

export function isIpAddress(ip: string) {
  // IP might be URL-encoded. fast-decode returns the input unchanged when there
  // is nothing to decode, and null (rather than throwing) on malformed input.
  const decodedIp = fastDecodeURIComponent(ip) ?? ip
  return ipAddr.Address6.isValid(decodedIp) || ipAddr.Address4.isValid(decodedIp)
}
