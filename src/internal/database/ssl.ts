import { isIP } from 'node:net'
import fastDecodeURIComponent from 'fast-decode-uri-component'
import { ConnectionOptions } from 'tls'

export function getSslSettings({
  connectionString,
  databaseSSLRootCert,
}: {
  connectionString: string
  databaseSSLRootCert: string | undefined
}): ConnectionOptions | undefined {
  if (!databaseSSLRootCert) return undefined

  // When connecting through PGBouncer, we connect through an IPv6 address rather than a hostname.
  // When passing in the root CA for SSL, this will always fail,
  // so we need to skip passing the SSL root cert if host name is an IP address.
  const hostname = getConnectionStringHostname(connectionString)
  if (hostname && isIpAddress(hostname)) {
    return { ca: databaseSSLRootCert, rejectUnauthorized: false }
  }

  return { ca: databaseSSLRootCert }
}

export function isIpAddress(ip: string) {
  const decoded = fastDecodeURIComponent(ip) ?? ip

  if (decoded.startsWith('[') && decoded.endsWith(']')) {
    return isIP(decoded.slice(1, -1)) !== 0
  }

  return isIP(decoded) !== 0
}

function getConnectionStringHostname(connectionString: string): string | undefined {
  const protocolSeparatorIndex = connectionString.indexOf('://')
  if (protocolSeparatorIndex === -1) {
    return undefined
  }

  const authorityStartIndex = protocolSeparatorIndex + 3
  let authorityEndIndex = connectionString.length

  for (let index = authorityStartIndex; index < connectionString.length; index++) {
    const charCode = connectionString.charCodeAt(index)
    if (charCode === 47 /* / */ || charCode === 63 /* ? */ || charCode === 35 /* # */) {
      authorityEndIndex = index
      break
    }
  }

  if (authorityStartIndex >= authorityEndIndex) {
    return undefined
  }

  let hostStartIndex = authorityStartIndex
  for (let index = authorityEndIndex - 1; index >= authorityStartIndex; index--) {
    if (connectionString.charCodeAt(index) === 64 /* @ */) {
      hostStartIndex = index + 1
      break
    }
  }

  if (hostStartIndex >= authorityEndIndex) {
    return undefined
  }

  if (connectionString.charCodeAt(hostStartIndex) === 91 /* [ */) {
    const bracketEndIndex = connectionString.indexOf(']', hostStartIndex + 1)

    if (
      bracketEndIndex === -1 ||
      bracketEndIndex >= authorityEndIndex ||
      (bracketEndIndex + 1 < authorityEndIndex &&
        connectionString.charCodeAt(bracketEndIndex + 1) !== 58) /* : */
    ) {
      return undefined
    }

    return connectionString.slice(hostStartIndex + 1, bracketEndIndex)
  }

  let hostEndIndex = authorityEndIndex
  for (let index = hostStartIndex; index < authorityEndIndex; index++) {
    if (connectionString.charCodeAt(index) === 58 /* : */) {
      hostEndIndex = index
      break
    }
  }

  if (hostStartIndex >= hostEndIndex) {
    return undefined
  }

  return connectionString.slice(hostStartIndex, hostEndIndex)
}
