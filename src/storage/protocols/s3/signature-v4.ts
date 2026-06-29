import { createHash } from 'node:crypto'
import { Writable } from 'node:stream'
import { ERRORS } from '@internal/errors'
import crypto from 'crypto'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'

export enum SignatureV4Service {
  S3 = 's3',
  S3VECTORS = 's3vectors',
}

interface SignatureV4Options {
  enforceRegion: boolean
  allowForwardedHeader?: boolean
  allowBodyHashing?: boolean
  nonCanonicalForwardedHost?: string
  publicUrl?: URL
  credentials: Omit<Credentials, 'shortDate'> & { secretKey: string }
}

export interface ClientSignature {
  credentials: Credentials
  signature: string
  signedHeaders: string[]
  sessionToken?: string
  longDate: string
  contentSha?: string
  policy?: {
    raw: string
    value: Policy
    fields: Record<string, string>
  }
}

interface SignatureRequest {
  url: string
  body?: string | ReadableStream | Buffer | Readable
  headers: Record<string, string | string[]>
  method: string
  query?: Record<string, string>
  prefix?: string
  payloadHasher?: Writable & { digestHex: () => string }
}

interface Credentials {
  accessKey: string
  shortDate: string
  region: string
  service: string
}

type SignatureHeaders = Record<string, string | string[] | undefined>
type SignatureQuery = Record<string, unknown>

export interface Policy {
  expiration: string
  conditions: PolicyCondition[]
}

/**
 * An AWS POST policy condition is either:
 *  - an exact-match object with a single key: `{ "bucket": "my-bucket" }`
 *  - a tuple: `["eq", "$key", "value"]`, `["starts-with", "$key", "prefix"]`,
 *    or `["content-length-range", min, max]`
 */
export type PolicyCondition = Record<string, string> | (string | number)[]

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

export const EMPTY_SHA256_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

/**
 * Submitted form fields that do NOT need to be covered by a policy condition,
 * keyed by their lowercased name. This matches AWS POST semantics: every other
 * field (including `key`, `bucket`, and every `x-amz-meta-*`) must be constrained
 * by a condition.
 */
const EXEMPT_POLICY_FIELDS = new Set([
  'policy',
  'x-amz-signature',
  'x-amz-algorithm',
  'x-amz-credential',
  'x-amz-date',
  'x-amz-security-token',
])

export class SignatureV4 {
  public readonly serverCredentials: SignatureV4Options['credentials']
  enforceRegion: boolean
  allowForwardedHeader?: boolean
  allowBodyHashing?: boolean
  nonCanonicalForwardedHost?: string
  publicUrl?: URL
  private readonly signingKeyCache = new Map<string, Buffer>()

  constructor(options: SignatureV4Options) {
    this.serverCredentials = options.credentials
    this.enforceRegion = options.enforceRegion
    this.allowForwardedHeader = options.allowForwardedHeader
    this.allowBodyHashing = options.allowBodyHashing
    this.nonCanonicalForwardedHost = options.nonCanonicalForwardedHost
    this.publicUrl = options.publicUrl
  }

  static parseAuthorizationHeader(headers: SignatureHeaders) {
    const clientSignature = headers.authorization
    if (typeof clientSignature !== 'string') {
      throw ERRORS.InvalidSignature('Missing authorization header')
    }

    const parts = clientSignature.split(' ')
    if (parts[0] !== 'AWS4-HMAC-SHA256') {
      throw ERRORS.InvalidSignature('Unsupported authorization type')
    }

    const params = this.extractClientSignature(clientSignature)
    const credentialPart = params.get('Credential')
    const signedHeadersPart = params.get('SignedHeaders')
    const signature = params.get('Signature')
    const longDate = headers['x-amz-date']
    const contentSha = coerceOptionalString(headers['x-amz-content-sha256'])
    const sessionToken = coerceOptionalString(headers['x-amz-security-token'])

    if (
      !isString(credentialPart) ||
      !isString(signedHeadersPart) ||
      !isString(signature) ||
      !isString(longDate)
    ) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }

    const signedHeaders = signedHeadersPart.split(';')
    const credentialsPart = credentialPart.split('/')

    if (credentialsPart.length !== 5) {
      throw ERRORS.InvalidSignature('Invalid credentials')
    }

    const [accessKey, shortDate, region, service] = credentialsPart
    return {
      credentials: { accessKey, shortDate, region, service },
      signedHeaders,
      signature,
      longDate,
      contentSha,
      sessionToken,
    }
  }

  static isChunkedUpload(headers: SignatureHeaders): boolean {
    const sha = headers['x-amz-content-sha256']
    if (typeof sha !== 'string') return false
    // If it exactly matches or starts with streaming prefix...
    return (
      sha.startsWith('STREAMING-AWS4-HMAC-SHA256-PAYLOAD') ||
      sha.startsWith('STREAMING-UNSIGNED-PAYLOAD')
    )
  }

  static parseQuerySignature(query: SignatureQuery) {
    const credentialPart = query['X-Amz-Credential']
    const signedHeaders = query['X-Amz-SignedHeaders']
    const signature = query['X-Amz-Signature']
    const longDate = query['X-Amz-Date']
    const contentSha = coerceOptionalString(query['X-Amz-Content-Sha256'])
    const sessionToken = coerceOptionalString(query['X-Amz-Security-Token'])
    const expires = coerceOptionalString(query['X-Amz-Expires'])

    if (
      !isString(credentialPart) ||
      !isString(signedHeaders) ||
      !isString(signature) ||
      !isString(longDate)
    ) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }

    if (expires) {
      this.checkExpiration(longDate, expires)
    }

    const credentialsPart = credentialPart.split('/')
    if (credentialsPart.length !== 5) {
      throw ERRORS.InvalidSignature('Invalid credentials')
    }

    const [accessKey, shortDate, region, service] = credentialsPart
    return {
      credentials: { accessKey, shortDate, region, service },
      signedHeaders: signedHeaders.split(';'),
      signature,
      longDate,
      contentSha,
      sessionToken,
    }
  }

  static parseMultipartSignature(form: FormData) {
    const credentialPart = form.get('X-Amz-Credential')
    const signature = form.get('X-Amz-Signature')
    const longDate = form.get('X-Amz-Date')
    const policy = form.get('Policy')
    const contentSha = coerceOptionalString(form.get('X-Amz-Content-Sha256'))
    const sessionToken = coerceOptionalString(form.get('X-Amz-Security-Token'))

    if (
      !isString(credentialPart) ||
      !isString(signature) ||
      !isString(policy) ||
      !isString(longDate)
    ) {
      throw ERRORS.InvalidSignature('Invalid signature format')
    }

    const xPolicy: Policy = JSON.parse(Buffer.from(policy, 'base64').toString('utf-8'))

    // A POST policy must declare an expiration; AWS treats a missing one as
    // invalid. Without this check a signed policy with no expiration would never
    // expire and could be replayed forever.
    this.checkPolicyExpiration(xPolicy.expiration)

    const fields: Record<string, string> = {}
    form.forEach((value, key) => {
      if (typeof value !== 'string') {
        return
      }
      const normalizedKey = key.toLowerCase()
      if (normalizedKey in fields) {
        throw ERRORS.InvalidSignature('Duplicate form field in POST policy request')
      }
      fields[normalizedKey] = value
    })

    const credentialsPart = credentialPart.split('/')
    if (credentialsPart.length !== 5) {
      throw ERRORS.InvalidSignature('Invalid credentials')
    }

    const [accessKey, shortDate, region, service] = credentialsPart
    return {
      credentials: { accessKey, shortDate, region, service },
      signedHeaders: [],
      signature,
      longDate,
      contentSha,
      sessionToken,
      policy: {
        raw: policy,
        value: xPolicy,
        fields,
      },
    }
  }

  /**
   * Validates a POST policy `expiration`, which is an absolute ISO8601 timestamp
   * (e.g. `2025-01-01T00:00:00Z`). Unlike {@link checkExpiration}, used by
   * presigned query URLs, where the value is a number of seconds relative to
   * X-Amz-Date.
   */
  protected static checkPolicyExpiration(expiration: string | undefined) {
    if (!expiration) {
      throw ERRORS.InvalidSignature('Missing policy expiration')
    }

    const expirationDate = new Date(expiration)
    if (isNaN(expirationDate.getTime())) {
      throw ERRORS.InvalidSignature('Invalid policy expiration')
    }

    if (expirationDate < new Date()) {
      throw ERRORS.ExpiredSignature()
    }
  }

  protected static checkExpiration(longDate: string, expires: string) {
    const expiresSec = parseInt(expires, 10)
    if (isNaN(expiresSec) || expiresSec < 0) {
      throw ERRORS.InvalidSignature('Invalid expiration')
    }

    const isoLongDate = longDate.replace(
      /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/,
      '$1-$2-$3T$4:$5:$6Z'
    )
    const requestDate = new Date(isoLongDate)
    const expirationDate = new Date(requestDate.getTime() + expiresSec * 1000)
    const isExpired = expirationDate < new Date()

    if (isExpired) {
      throw ERRORS.ExpiredSignature()
    }
  }

  protected static extractClientSignature(clientSignature: string) {
    return clientSignature
      .replace('AWS4-HMAC-SHA256 ', '')
      .split(',')
      .reduce((values, value) => {
        const [k, v] = value.split('=')
        values.set(k.trim(), v)
        return values
      }, new Map<string, string>())
  }

  /**
   * Verify if client signature and server signature matches
   * @param clientSignature
   * @param request
   */
  async verify(clientSignature: ClientSignature, request: SignatureRequest) {
    if (typeof clientSignature.policy?.raw === 'string') {
      return this.verifyPostPolicySignature(clientSignature, clientSignature.policy.raw)
    }

    const serverSignature = await this.sign(clientSignature, request)
    return crypto.timingSafeEqual(
      Buffer.from(clientSignature.signature),
      Buffer.from(serverSignature.signature)
    )
  }

  /**
   * Evaluates the signed POST policy conditions against the values the client
   * actually submitted, in a single pass, and:
   *  - throws if any condition is not satisfied;
   *  - throws if any submitted field is NOT covered by a condition
   *  - returns the effective `content-length-range` upper bound
   *
   * The `bucket` value is taken from the request target (the URL), since AWS
   * POST uploads carry the bucket in the path rather than as a form field.
   */
  validatePostPolicyConditions(
    clientSignature: ClientSignature,
    opts: { bucket?: string }
  ): number | undefined {
    const policy = clientSignature.policy
    if (!policy) {
      return undefined
    }

    const conditions = policy.value?.conditions
    if (!Array.isArray(conditions)) {
      throw ERRORS.InvalidSignature('Invalid policy conditions')
    }

    const fields: Record<string, string> = { ...policy.fields }
    if (opts.bucket !== undefined) {
      fields['bucket'] = opts.bucket
    }

    const coveredFields = new Set<string>()
    let max: number | undefined

    for (const condition of conditions) {
      const result = this.evaluatePolicyCondition(condition, fields)
      if (result.field) {
        coveredFields.add(result.field)
      }
      if (result.max !== undefined) {
        // Fold to the most restrictive upper bound across every content-length-range.
        max = max === undefined ? result.max : Math.min(max, result.max)
      }
    }

    // Every submitted field must be constrained by a condition
    for (const field of Object.keys(fields)) {
      if (EXEMPT_POLICY_FIELDS.has(field) || field.startsWith('x-ignore-')) {
        continue
      }
      if (!coveredFields.has(field)) {
        throw ERRORS.AccessDenied(
          `Policy condition failed: field "${field}" is not covered by the policy`
        )
      }
    }

    return max
  }

  protected evaluatePolicyCondition(
    condition: PolicyCondition,
    fields: Record<string, string>
  ): { field?: string; max?: number } {
    const normalized = this.normalizePolicyCondition(condition)

    if (normalized.operator === 'content-length-range') {
      return { max: normalized.max }
    }

    if (normalized.operator === 'eq') {
      this.assertFieldEquals(normalized.field, normalized.value, fields)
    } else if (normalized.operator === 'starts-with') {
      this.assertFieldStartsWith(normalized.field, normalized.value, fields)
    }

    return { field: normalized.field }
  }

  /**
   * Normalize a raw policy condition into a single operator form, or throws
   * if it is malformed. The exact-match object `{ field: value }` is the same
   * as `["eq", "$field", value]`, so both collapse to the same `eq` shape.
   */
  private normalizePolicyCondition(
    condition: PolicyCondition
  ):
    | { operator: 'eq' | 'starts-with'; field: string; value: string | number }
    | { operator: 'content-length-range'; min: number; max: number } {
    // Exact-match object form: { "field": "value" } with exactly one key.
    if (condition && typeof condition === 'object' && !Array.isArray(condition)) {
      const keys = Object.keys(condition)
      if (keys.length !== 1) {
        throw ERRORS.InvalidSignature('Invalid policy condition')
      }
      return { operator: 'eq', field: keys[0].toLowerCase(), value: condition[keys[0]] }
    }

    // Tuple form: ["eq" | "starts-with", "$field", value] or
    // ["content-length-range", min, max].
    if (Array.isArray(condition)) {
      const [operator, target, value] = condition

      if (operator === 'content-length-range') {
        if (typeof target !== 'number' || typeof value !== 'number') {
          throw ERRORS.InvalidSignature('Invalid content-length-range condition')
        }
        return { operator: 'content-length-range', min: target, max: value }
      }

      if (
        (operator === 'eq' || operator === 'starts-with') &&
        typeof target === 'string' &&
        target.startsWith('$')
      ) {
        return { operator, field: target.slice(1).toLowerCase(), value }
      }
    }

    throw ERRORS.InvalidSignature('Unsupported policy condition')
  }

  private assertFieldEquals(
    field: string,
    expected: string | number,
    fields: Record<string, string>
  ) {
    const actual = fields[field]
    if (actual === undefined || actual !== String(expected)) {
      throw ERRORS.AccessDenied(`Policy condition failed: "${field}" does not match`)
    }
  }

  private assertFieldStartsWith(
    field: string,
    prefix: string | number,
    fields: Record<string, string>
  ) {
    const actual = fields[field]
    if (actual === undefined || !actual.startsWith(String(prefix ?? ''))) {
      throw ERRORS.AccessDenied(
        `Policy condition failed: "${field}" does not start with "${prefix}"`
      )
    }
  }

  /**
   * Verifies signature for POST upload requests
   * @param clientSignature
   * @param policy
   */
  verifyPostPolicySignature(clientSignature: ClientSignature, policy: string) {
    const serverSignature = this.signPostPolicy(clientSignature, policy)
    return crypto.timingSafeEqual(
      Buffer.from(clientSignature.signature),
      Buffer.from(serverSignature)
    )
  }

  public validateChunkSignature(
    clientSignature: ClientSignature,
    chunkHash: string,
    chunkSignature: string,
    prevSignature: string = clientSignature.signature
  ): boolean {
    const { shortDate, region, service } = clientSignature.credentials
    const signingKey = this.getCachedSigningKey(shortDate, region, service)

    // Build the “String to Sign” for this chunk exactly per AWS:
    //    AWS4-HMAC-SHA256-PAYLOAD
    //    <longDate>
    //    <shortDate/region/service/aws4_request>
    //    <prevSignature>
    //    SHA256("")   ← the hash of the empty string
    //    SHA256(chunkData)
    const scope = `${shortDate}/${region}/${service}/aws4_request`
    const stringToSign = [
      'AWS4-HMAC-SHA256-PAYLOAD',
      clientSignature.longDate,
      scope,
      prevSignature,
      EMPTY_SHA256_HASH,
      chunkHash,
    ].join('\n')

    // 4) HMAC it with the derived key and compare
    const expected = this.hmac(signingKey, stringToSign)

    return crypto.timingSafeEqual(expected, Buffer.from(chunkSignature, 'hex'))
  }

  signPostPolicy(clientSignature: ClientSignature, policy: string) {
    const serverCredentials = this.serverCredentials

    this.validateCredentials(clientSignature.credentials)
    const selectedRegion = this.getSelectedRegion(clientSignature.credentials.region)

    const signingKey = this.getCachedSigningKey(
      clientSignature.credentials.shortDate,
      selectedRegion,
      serverCredentials.service
    )

    return this.hmac(signingKey, policy).toString('hex')
  }

  /**
   * Sign the server side signature
   * @param clientSignature
   * @param request
   */
  async sign(clientSignature: ClientSignature, request: SignatureRequest) {
    const serverCredentials = this.serverCredentials

    this.validateCredentials(clientSignature.credentials)

    const longDate = clientSignature.longDate
    if (!longDate) {
      throw ERRORS.AccessDenied('No date provided')
    }

    const selectedRegion = this.getSelectedRegion(clientSignature.credentials.region)
    const canonicalRequest = await this.constructCanonicalRequest(
      clientSignature,
      request,
      clientSignature.signedHeaders
    )

    const stringToSign = this.constructStringToSign(
      longDate,
      clientSignature.credentials.shortDate,
      selectedRegion,
      serverCredentials.service,
      canonicalRequest
    )

    const signingKey = this.getCachedSigningKey(
      clientSignature.credentials.shortDate,
      selectedRegion,
      serverCredentials.service
    )

    return { signature: this.hmac(signingKey, stringToSign).toString('hex'), canonicalRequest }
  }

  protected async getPayloadHash(clientSignature: ClientSignature, request: SignatureRequest) {
    const body = request.body

    // For presigned URLs and GET requests, use UNSIGNED-PAYLOAD
    if (request.query && request.query['X-Amz-Signature'] && request.method === 'GET') {
      return 'UNSIGNED-PAYLOAD'
    }

    // If contentSha is provided, use it
    if (clientSignature.contentSha) {
      return clientSignature.contentSha
    }

    // If the body is undefined, use the hash of an empty string
    if (body === null || body === undefined) {
      return EMPTY_SHA256_HASH
    }

    // Calculate the SHA256 hash of the body
    if (typeof body === 'string' || ArrayBuffer.isView(body)) {
      return crypto
        .createHash('sha256')
        .update(typeof body === 'string' ? body : Buffer.from(body.buffer))
        .digest('hex')
    }

    // If body is a ReadableStream, calculate the SHA256 hash of the stream
    if (body instanceof Readable && this.allowBodyHashing && request.payloadHasher) {
      return await pipeline(body, request.payloadHasher).then(() => {
        return request.payloadHasher?.digestHex()
      })
    }

    // Default to UNSIGNED-PAYLOAD if body is not a string or ArrayBuffer
    return 'UNSIGNED-PAYLOAD'
  }

  protected async constructCanonicalRequest(
    clientSignature: ClientSignature,
    request: SignatureRequest,
    signedHeaders: string[]
  ) {
    const method = request.method
    const prefix = request.prefix ? request.prefix.replace(/\/+$/, '') : ''
    const canonicalUri = new URL(`http://localhost:8080${prefix}${request.url}`).pathname
    const canonicalQueryString = this.constructCanonicalQueryString(request.query || {})
    const canonicalHeaders = this.constructCanonicalHeaders(request, signedHeaders)
    const signedHeadersString = signedHeaders.sort().join(';')
    const payloadHash = await this.getPayloadHash(clientSignature, request)

    return `${method}\n${canonicalUri}\n${canonicalQueryString}\n${canonicalHeaders}\n${signedHeadersString}\n${payloadHash}`
  }

  /**
   * Encodes a URI component according to RFC 3986, as required by AWS Signature V4.
   * This differs from encodeURIComponent which doesn't encode certain characters
   * like parentheses that AWS requires to be percent-encoded.
   */
  protected encodeRFC3986URIComponent(str: string): string {
    return encodeURIComponent(str).replace(/[!'()*]/g, (c) => {
      return '%' + c.charCodeAt(0).toString(16).toUpperCase()
    })
  }

  protected constructCanonicalQueryString(query: Record<string, string>) {
    return Object.keys(query)
      .filter((key) => !(key in ALWAYS_UNSIGNABLE_QUERY_PARAMS))
      .sort()
      .map(
        (key) =>
          `${this.encodeRFC3986URIComponent(key)}=${this.encodeRFC3986URIComponent(
            query[key] as string
          )}`
      )
      .join('&')
  }

  protected constructCanonicalHeaders(request: SignatureRequest, signedHeaders: string[]) {
    return (
      signedHeaders
        .filter(
          (header) =>
            request.headers[header] !== undefined &&
            !(header.toLowerCase() in ALWAYS_UNSIGNABLE_HEADERS)
        )
        .sort()
        .map((header) => {
          if (header === 'host') {
            return this.getHostHeader(request)
          }

          if (header === 'content-length') {
            const headerValue = this.getHeader(request, header) ?? '0'
            return `${header}:${headerValue}`
          }

          // cloudflare modifies accept-encoding header which causes signing to fail
          // instead use x-original-accept-encoding if available
          if (header === 'accept-encoding') {
            const originalEncoding = this.getHeader(request, 'x-original-accept-encoding')
            if (originalEncoding) {
              return `${header}:${originalEncoding}`
            }
          }

          return `${header}:${this.getHeader(request, header)}`
        })
        .join('\n') + '\n'
    )
  }

  protected getHostHeader(request: SignatureRequest) {
    // When a public URL is configured, use its host for signature verification.
    // This avoids proxy header issues (e.g., Kong overwriting X-Forwarded-Port).
    if (this.publicUrl) {
      return `host:${this.publicUrl.host}`
    }

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
      const xForwardedHost = this.getHeader(request, this.nonCanonicalForwardedHost.toLowerCase())
      if (xForwardedHost) {
        return `host:${xForwardedHost.toLowerCase()}`
      }
    }

    const xForwardedHost = this.getHeader(request, 'x-forwarded-host')
    if (xForwardedHost) {
      const port = this.getHeader(request, 'x-forwarded-port')
      const host = `host:${xForwardedHost.toLowerCase()}`

      if (port && !['443', '80'].includes(port)) {
        if (!xForwardedHost.includes(':')) {
          return host + ':' + port
        } else {
          return 'host:' + xForwardedHost.replace(/:\d+$/, `:${port}`)
        }
      }
      return host
    }

    return `host:${this.getHeader(request, 'host')}`
  }

  protected validateCredentials(credentials: Credentials) {
    if (credentials.accessKey !== this.serverCredentials.accessKey) {
      throw ERRORS.AccessDenied('Invalid Access Key')
    }

    if (this.enforceRegion && credentials.region !== this.serverCredentials.region) {
      throw ERRORS.AccessDenied('Invalid Region')
    }

    if (credentials.service !== this.serverCredentials.service) {
      throw ERRORS.AccessDenied('Invalid Service')
    }
  }

  protected getSelectedRegion(clientRegion: string) {
    if (
      !this.enforceRegion &&
      ['auto', 'us-east-1', this.serverCredentials.region, ''].includes(clientRegion)
    ) {
      return clientRegion
    }
    return this.serverCredentials.region
  }

  protected constructStringToSign(
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

  protected signingKey(
    key: string,
    dateStamp: string,
    regionName: string,
    serviceName: string
  ): Buffer {
    const kDate = this.hmac(`AWS4${key}`, dateStamp)
    const kRegion = this.hmac(kDate, regionName)
    const kService = this.hmac(kRegion, serviceName)
    return this.hmac(kService, 'aws4_request')
  }

  private getCachedSigningKey(dateStamp: string, regionName: string, serviceName: string) {
    const cacheKey = `${dateStamp}\0${regionName}\0${serviceName}`
    let signingKey = this.signingKeyCache.get(cacheKey)

    if (!signingKey) {
      signingKey = this.signingKey(
        this.serverCredentials.secretKey,
        dateStamp,
        regionName,
        serviceName
      )
      this.signingKeyCache.set(cacheKey, signingKey)
    }

    return signingKey
  }

  protected async sha256OfRequest(req: Readable) {
    const hash = createHash('sha256')
    for await (const chunk of req) {
      hash.update(chunk)
    }
    return hash.digest('hex')
  }

  protected hmac(key: string | Buffer, data: string): Buffer {
    return crypto.createHmac('sha256', key).update(data).digest()
  }

  protected getHeader(request: SignatureRequest, name: string) {
    const item = request.headers[name]
    if (Array.isArray(item)) {
      return item.join(',')
    }
    return item
  }
}

function isString(value: unknown): value is string {
  return typeof value === 'string'
}

function coerceOptionalString(value: unknown): string | undefined {
  if (value === undefined || value === null) {
    return undefined
  }

  return String(value)
}
