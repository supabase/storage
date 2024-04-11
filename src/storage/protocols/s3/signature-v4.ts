import crypto from 'crypto'
import { ERRORS } from '../../errors'

interface SignatureV4Options {
  enforceRegion: boolean
  credentials: Omit<Credentials, 'shortDate'> & { secretKey: string }
}

export interface ClientSignature {
  credentials: Credentials
  signature: string
  signedHeaders: string[]
}

interface SignatureRequest {
  url: string
  body?: string | ReadableStream | Buffer
  headers: Record<string, string | string[]>
  method: string
  query?: Record<string, string>
  prefix?: string
  credentials: Credentials
  signature: string
  signedHeaders: string[]
}

interface Credentials {
  accessKey: string
  shortDate: string
  region: string
  service: string
}

/**
 * Lists the headers that should never be included in the
 * request signature signature process.
 */
export const ALWAYS_UNSIGNABLE_HEADERS = {
  authorization: true,
  'cache-control': true,
  connection: true,
  expect: true,
  from: true,
  'keep-alive': true,
  'max-forwards': true,
  pragma: true,
  referer: true,
  te: true,
  trailer: true,
  'transfer-encoding': true,
  upgrade: true,
  'user-agent': true,
  'x-amzn-trace-id': true,
}

export class SignatureV4 {
  public readonly serverCredentials: SignatureV4Options['credentials']
  enforceRegion: boolean

  constructor(options: SignatureV4Options) {
    this.serverCredentials = options.credentials
    this.enforceRegion = options.enforceRegion
  }

  static parseAuthorizationHeader(header: string) {
    const parts = header.split(' ')
    if (parts[0] !== 'AWS4-HMAC-SHA256') {
      throw ERRORS.InvalidSignature('Unsupported authorization type')
    }

    const params = header
      .replace('AWS4-HMAC-SHA256 ', '')
      .split(',')
      .reduce((values, value) => {
        const [k, v] = value.split('=')
        values.set(k.trim(), v)
        return values
      }, new Map<string, string>())

    const credentialPart = params.get('Credential')
    const signedHeadersPart = params.get('SignedHeaders')
    const signaturePart = params.get('Signature')

    if (!credentialPart || !signedHeadersPart || !signaturePart) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }
    const signedHeaders = signedHeadersPart.split(';') || []

    const credentialsPart = credentialPart.split('/')

    if (credentialsPart.length !== 5) {
      throw ERRORS.InvalidSignature('Invalid credentials')
    }

    const [accessKey, shortDate, region, service] = credentialsPart

    return {
      credentials: {
        accessKey,
        shortDate,
        region,
        service,
      },
      signedHeaders,
      signature: signaturePart,
    }
  }

  verify(request: SignatureRequest) {
    const { clientSignature, serverSignature } = this.sign(request)
    // Compare the computed signature with the provided signature
    return crypto.timingSafeEqual(Buffer.from(clientSignature), Buffer.from(serverSignature))
  }

  sign(request: SignatureRequest) {
    const authorizationHeader = this.getHeader(request, 'authorization')
    if (!authorizationHeader) {
      throw ERRORS.AccessDenied('Missing authorization header')
    }

    if (request.credentials.accessKey !== this.serverCredentials.accessKey) {
      throw ERRORS.AccessDenied('Invalid Access Key')
    }

    // Ensure the region and service match the expected values
    if (this.enforceRegion && request.credentials.region !== this.serverCredentials.region) {
      throw ERRORS.AccessDenied('Invalid Region')
    }

    if (request.credentials.service !== this.serverCredentials.service) {
      throw ERRORS.AccessDenied('Invalid Service')
    }

    const longDate = request.headers['x-amz-date'] as string
    if (!longDate) {
      throw ERRORS.AccessDenied('No date header provided')
    }

    // When enforcing region is false, we allow the region to be:
    // - auto
    // - us-east-1
    // - the region set in the env
    if (
      !this.enforceRegion &&
      !['auto', 'us-east-1', this.serverCredentials.region, ''].includes(request.credentials.region)
    ) {
      throw ERRORS.AccessDenied('Invalid Region')
    }

    const selectedRegion = this.enforceRegion
      ? this.serverCredentials.region
      : request.credentials.region

    // Construct the Canonical Request and String to Sign
    const canonicalRequest = this.constructCanonicalRequest(request, request.signedHeaders)
    const stringToSign = this.constructStringToSign(
      longDate,
      request.credentials.shortDate,
      selectedRegion,
      this.serverCredentials.service,
      canonicalRequest
    )

    const signingKey = this.signingKey(
      this.serverCredentials.secretKey,
      request.credentials.shortDate,
      selectedRegion,
      this.serverCredentials.service
    )

    return {
      clientSignature: request.signature,
      serverSignature: this.hmac(signingKey, stringToSign).toString('hex'),
    }
  }

  getPayloadHash(request: SignatureRequest) {
    const headers = request.headers
    const body = request.body

    for (const headerName of Object.keys(headers)) {
      if (headerName.toLowerCase() === 'x-amz-content-sha256') {
        return headers[headerName]
      }
    }

    const contentLenght = parseInt(this.getHeader(request, 'content-length') || '0', 10)
    let payloadHash = ''

    if (body === undefined && contentLenght === 0) {
      payloadHash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    } else if (typeof body === 'string' || ArrayBuffer.isView(body)) {
      payloadHash = crypto
        .createHash('sha256')
        .update(typeof body === 'string' ? JSON.stringify(body) : Buffer.from(body.buffer))
        .digest('hex')
    } else {
      payloadHash = 'UNSIGNED-PAYLOAD'
    }

    return payloadHash
  }

  constructStringToSign(
    date: string,
    dateStamp: string,
    region: string,
    service: string,
    canonicalRequest: string
  ) {
    const algorithm = 'AWS4-HMAC-SHA256'
    const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`
    const hashedCanonicalRequest = crypto
      .createHash('sha256')
      .update(canonicalRequest)
      .digest('hex')

    return `${algorithm}\n${date}\n${credentialScope}\n${hashedCanonicalRequest}`
  }

  hmac(key: string | Buffer, data: string): Buffer {
    return crypto.createHmac('sha256', key).update(data).digest()
  }

  protected signingKey(
    key: string,
    dateStamp: string,
    regionName: string,
    serviceName: string
  ): Buffer {
    const kDate = this.hmac('AWS4' + key, dateStamp)
    const kRegion = this.hmac(kDate, regionName)
    const kService = this.hmac(kRegion, serviceName)
    return this.hmac(kService, 'aws4_request')
  }

  protected constructCanonicalRequest(request: SignatureRequest, signedHeaders: string[]) {
    const method = request.method
    const canonicalUri = new URL(`http://localhost:8080${request.prefix || ''}${request.url}`)
      .pathname

    const canonicalQueryString = Object.keys((request.query as object) || {})
      .sort()
      .map(
        (key) =>
          `${encodeURIComponent(key)}=${encodeURIComponent((request.query as any)[key] as string)}`
      )
      .join('&')

    const canonicalHeaders =
      signedHeaders
        .filter(
          (header) =>
            request.headers[header] !== undefined &&
            !(header.toLowerCase() in ALWAYS_UNSIGNABLE_HEADERS)
        )
        .sort()
        .map((header) => {
          if (header === 'host') {
            const xForwardedHost = this.getHeader(request, 'x-forwarded-host')
            if (xForwardedHost) {
              return `host:${xForwardedHost.toLowerCase()}`
            }
          }

          return `${header.toLowerCase()}:${
            (request.headers[header.toLowerCase()] || '') as string
          }`
        })
        .join('\n') + '\n'

    const signedHeadersString = signedHeaders.sort().join(';')

    const payloadHash = this.getPayloadHash(request)

    return `${method}\n${canonicalUri}\n${canonicalQueryString}\n${canonicalHeaders}\n${signedHeadersString}\n${payloadHash}`
  }

  protected getHeader(request: SignatureRequest, name: string) {
    const item = request.headers[name]
    if (Array.isArray(item)) {
      return item.join(',')
    }
    return item
  }
}
