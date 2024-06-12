import crypto from 'crypto'
import { ERRORS } from '../../errors'

interface SignatureV4Options {
  enforceRegion: boolean
  allowForwardedHeader?: boolean
  nonCanonicalForwardedHost?: string
  credentials: Omit<Credentials, 'shortDate'> & { secretKey: string }
}

export interface ClientSignature {
  credentials: Credentials
  signature: string
  signedHeaders: string[]
  sessionToken?: string
  longDate: string
  contentSha?: string
}

interface SignatureRequest {
  url: string
  body?: string | ReadableStream | Buffer
  headers: Record<string, string | string[]>
  method: string
  query?: Record<string, string>
  prefix?: string
  clientSignature: ClientSignature
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

export const ALWAYS_UNSIGNABLE_QUERY_PARAMS = {
  'X-Amz-Signature': true,
}

export class SignatureV4 {
  public readonly serverCredentials: SignatureV4Options['credentials']
  enforceRegion: boolean
  allowForwardedHeader?: boolean
  nonCanonicalForwardedHost?: string

  constructor(options: SignatureV4Options) {
    this.serverCredentials = options.credentials
    this.enforceRegion = options.enforceRegion
    this.allowForwardedHeader = options.allowForwardedHeader
    this.nonCanonicalForwardedHost = options.nonCanonicalForwardedHost
  }

  static parseAuthorizationHeader(headers: Record<string, any>) {
    const clientSignature = headers.authorization

    if (typeof clientSignature !== 'string') {
      throw ERRORS.InvalidSignature('Missing authorization header')
    }

    const parts = clientSignature.split(' ')
    if (parts[0] !== 'AWS4-HMAC-SHA256') {
      throw ERRORS.InvalidSignature('Unsupported authorization type')
    }

    const params = clientSignature
      .replace('AWS4-HMAC-SHA256 ', '')
      .split(',')
      .reduce((values, value) => {
        const [k, v] = value.split('=')
        values.set(k.trim(), v)
        return values
      }, new Map<string, string>())

    const credentialPart = params.get('Credential')
    const signedHeadersPart = params.get('SignedHeaders')
    const signature = params.get('Signature')
    const longDate = headers['x-amz-date']
    const contentSha = headers['x-amz-content-sha256']
    const sessionToken = headers['x-amz-security-token']

    if (!validateTypeOfStrings(credentialPart, signedHeadersPart, signature, longDate)) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }

    const signedHeaders = signedHeadersPart?.split(';') || []

    const credentialsPart = credentialPart?.split('/') || []

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
      signature,
      longDate,
      contentSha,
      sessionToken,
    }
  }

  static parseQuerySignature(query: Record<string, any>) {
    const credentialPart = query['X-Amz-Credential']
    const signedHeaders = query['X-Amz-SignedHeaders']
    const signature = query['X-Amz-Signature']
    const longDate = query['X-Amz-Date']
    const contentSha = query['X-Amz-Content-Sha256']
    const sessionToken = query['X-Amz-Security-Token']
    const expires = query['X-Amz-Expires']

    if (!validateTypeOfStrings(credentialPart, signedHeaders, signature)) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }

    if (expires) {
      const expiresSec = parseInt(expires, 10)
      if (isNaN(expiresSec) || expiresSec < 0) {
        throw ERRORS.InvalidSignature('Invalid expiration')
      }

      if (typeof longDate !== 'string') {
        throw ERRORS.InvalidSignature('Invalid date')
      }

      const isoLongDate = longDate.replace(
        /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/,
        '$1-$2-$3T$4:$5:$6Z'
      )

      const expirationDate = new Date(isoLongDate)

      if (isNaN(expirationDate.getTime())) {
        throw ERRORS.InvalidSignature('Invalid date')
      }
      expirationDate.setSeconds(expirationDate.getSeconds() + expiresSec)

      const isExpired = expirationDate < new Date()
      if (isExpired) {
        throw ERRORS.ExpiredSignature()
      }
    }

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
      signedHeaders: signedHeaders.split(';'),
      signature,
      longDate,
      contentSha,
      sessionToken,
    }
  }

  verify(request: SignatureRequest) {
    const { clientSignature, serverSignature } = this.sign(request)
    // Compare the computed signature with the provided signature
    return crypto.timingSafeEqual(Buffer.from(clientSignature), Buffer.from(serverSignature))
  }

  sign(request: SignatureRequest) {
    if (request.clientSignature.credentials.accessKey !== this.serverCredentials.accessKey) {
      throw ERRORS.AccessDenied('Invalid Access Key')
    }

    // Ensure the region and service match the expected values
    if (
      this.enforceRegion &&
      request.clientSignature.credentials.region !== this.serverCredentials.region
    ) {
      throw ERRORS.AccessDenied('Invalid Region')
    }

    if (request.clientSignature.credentials.service !== this.serverCredentials.service) {
      throw ERRORS.AccessDenied('Invalid Service')
    }

    const longDate = request.clientSignature.longDate
    if (!longDate) {
      throw ERRORS.AccessDenied('No date header provided')
    }

    // When enforcing region is false, we allow the region to be:
    // - auto
    // - us-east-1
    // - the region set in the env
    if (
      !this.enforceRegion &&
      !['auto', 'us-east-1', this.serverCredentials.region, ''].includes(
        request.clientSignature.credentials.region
      )
    ) {
      throw ERRORS.AccessDenied('Invalid Region')
    }

    const selectedRegion = this.enforceRegion
      ? this.serverCredentials.region
      : request.clientSignature.credentials.region

    // Construct the Canonical Request and String to Sign
    const canonicalRequest = this.constructCanonicalRequest(
      request,
      request.clientSignature.signedHeaders
    )
    const stringToSign = this.constructStringToSign(
      longDate,
      request.clientSignature.credentials.shortDate,
      selectedRegion,
      this.serverCredentials.service,
      canonicalRequest
    )

    const signingKey = this.signingKey(
      this.serverCredentials.secretKey,
      request.clientSignature.credentials.shortDate,
      selectedRegion,
      this.serverCredentials.service
    )

    return {
      clientSignature: request.clientSignature.signature,
      serverSignature: this.hmac(signingKey, stringToSign).toString('hex'),
    }
  }

  getPayloadHash(request: SignatureRequest) {
    const body = request.body

    if (request.clientSignature.contentSha) {
      return request.clientSignature.contentSha
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
      .filter((key) => !(key in ALWAYS_UNSIGNABLE_QUERY_PARAMS))
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
            if (this.allowForwardedHeader) {
              const forwarded = this.getHeader(request, 'forwarded')
              if (forwarded) {
                const extractedHost = /host="?([^";]+)/.exec(forwarded)?.[1]
                if (extractedHost) {
                  return `host:${extractedHost.toLowerCase()}`
                }
              }
            }

            if (this.nonCanonicalForwardedHost) {
              const xForwardedHost = this.getHeader(
                request,
                this.nonCanonicalForwardedHost.toLowerCase()
              )

              if (xForwardedHost) {
                return `host:${xForwardedHost.toLowerCase()}`
              }
            }

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

function validateTypeOfStrings(...values: any[]) {
  return values.every((value) => {
    return typeof value === 'string'
  })
}
