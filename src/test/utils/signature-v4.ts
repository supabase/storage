import { createHash } from 'node:crypto'
import { Hash } from '@smithy/hash-node'
import { HttpRequest } from '@smithy/protocol-http'
import { SignatureV4 } from '@smithy/signature-v4'
import { SignatureV4 as ServerSignatureV4, SignatureV4Service } from '@storage/protocols/s3'
import { getConfig } from '../../config'

const { s3ProtocolAccessKeyId, s3ProtocolAccessKeySecret, storageS3Region } = getConfig()

interface RawSignatureV4Credentials {
  accessKeyId: string
  secretAccessKey: string
  sessionToken?: string
  region: string
  service: string
}

export function createRawSignatureV4Signer(
  credentials: RawSignatureV4Credentials,
  options: {
    hostname?: string
    method?: string
  } = {}
) {
  const hostname = options.hostname ?? 'storage.test'
  const method = options.method ?? 'HEAD'
  const signer = new SignatureV4({
    credentials,
    region: credentials.region,
    service: credentials.service,
    sha256: Hash.bind(null, 'sha256'),
    uriEscapePath: false,
  })

  return (path: string, headers: Record<string, string> = {}) =>
    signer.sign(
      new HttpRequest({
        protocol: 'http:',
        hostname,
        method,
        path,
        query: {},
        headers: {
          host: hostname,
          ...headers,
        },
      })
    )
}

export function formatAwsDate(date = new Date()) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, '')
}

export function sha256Hex(value: Buffer) {
  return createHash('sha256').update(value).digest('hex')
}

/**
 * Signs a request the way a real S3 client would, giving full control over exactly which
 * headers end up in `SignedHeaders` (only the headers passed in `options.headers`, plus the
 * always-signed host/x-amz-date/x-amz-content-sha256). Useful for simulating an attacker who
 * appends an extra, unsigned header to an otherwise legitimately-signed request.
 */
export async function createSignedS3Request(options: {
  baseUrl: string
  path: string
  method: 'POST' | 'PUT' | 'GET' | 'DELETE'
  body?: string | Buffer
  query?: Record<string, string>
  headers?: Record<string, string>
  contentSha?: string
  includeContentLength?: boolean
}) {
  const longDate = formatAwsDate()
  const shortDate = longDate.slice(0, 8)
  const host = new URL(options.baseUrl).host
  const service = SignatureV4Service.S3
  const payload = options.body ?? Buffer.alloc(0)
  const payloadBuffer = typeof payload === 'string' ? Buffer.from(payload) : payload
  const payloadHash = options.contentSha || sha256Hex(payloadBuffer)
  const normalizedHeaders = Object.fromEntries(
    Object.entries(options.headers || {}).map(([key, value]) => [key.toLowerCase(), value])
  )
  const signer = new ServerSignatureV4({
    enforceRegion: false,
    credentials: {
      accessKey: s3ProtocolAccessKeyId!,
      secretKey: s3ProtocolAccessKeySecret!,
      region: storageS3Region,
      service,
    },
  })

  const headers: Record<string, string> = {
    host,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': longDate,
    ...(options.includeContentLength ? { 'content-length': payloadBuffer.length.toString() } : {}),
    ...normalizedHeaders,
  }

  const signedHeaders = Object.keys(headers)
    .map((header) => header.toLowerCase())
    .sort()

  const clientSignature = {
    credentials: {
      accessKey: s3ProtocolAccessKeyId!,
      shortDate,
      region: storageS3Region,
      service,
    },
    signedHeaders,
    signature: '',
    longDate,
    contentSha: payloadHash,
  }

  const {
    signatures: [signature],
  } = await signer.sign(clientSignature, {
    url: options.path,
    method: options.method,
    headers,
    query: options.query,
    body: options.body,
  })

  const requestUrl = new URL(`${options.baseUrl}${options.path}`)

  if (options.query) {
    for (const [key, value] of Object.entries(options.query)) {
      requestUrl.searchParams.set(key, value)
    }
  }

  return {
    headers: {
      ...headers,
      authorization:
        `AWS4-HMAC-SHA256 Credential=${s3ProtocolAccessKeyId}/${shortDate}/` +
        `${storageS3Region}/${service}/aws4_request, SignedHeaders=${signedHeaders.join(';')}, ` +
        `Signature=${signature}`,
    },
    requestUrl,
    service,
    shortDate,
    longDate,
    signature,
  }
}
