import { logger } from '@internal/monitoring'

export function getSslSettings({
  connectionString,
  databaseSSLRootCert,
}: {
  connectionString: string
  databaseSSLRootCert: string | undefined
}): { ca: string } | undefined {
  if (!databaseSSLRootCert) return undefined

  try {
    // When connecting through PGBouncer, we connect through an IPv6 address rather than a hostname
    // When passing in the root CA for SSL, this will always fail, so we need to skip passing in the SSL root cert
    // in case the hostname is an IP address
    const url = new URL(connectionString)
    if (url.hostname && isIpAddress(url.hostname)) {
      return undefined
    }
  } catch (err) {
    // ignore to ensure this never breaks the connection in case of an invalid URL
    logger.warn(err, 'Failed to parse connection string')
  }

  return { ca: databaseSSLRootCert }
}

export function isIpAddress(ip: string) {
  const ipv4Pattern = /^(\d{1,3}\.){3}\d{1,3}$/
  const ipv6Pattern = /^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/
  return ipv4Pattern.test(ip) || ipv6Pattern.test(ip)
}
