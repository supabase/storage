export function getSslSettings(
  connectionString: string,
  rootCert?: string
): { ca?: string; rejectUnauthorized?: boolean } | false | undefined {
  const sslMode = getSslMode(connectionString)

  if (sslMode === 'disable') {
    return false
  }

  if (rootCert) {
    return {
      ca: rootCert,
    }
  }

  if (sslMode === 'require' || sslMode === 'prefer') {
    return {
      rejectUnauthorized: false,
    }
  }

  return undefined
}

function getSslMode(connectionString: string): string | undefined {
  try {
    return new URL(connectionString).searchParams.get('sslmode') || undefined
  } catch {
    return undefined
  }
}
