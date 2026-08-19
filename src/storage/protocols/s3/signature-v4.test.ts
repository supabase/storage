import { createHash, createHmac } from 'node:crypto'
import { createServer, request as httpRequest, type IncomingHttpHeaders } from 'node:http'
import { type AddressInfo } from 'node:net'
import { HeadObjectCommand, S3Client } from '@aws-sdk/client-s3'
import {
  EMPTY_SHA256_HASH,
  SignatureV4,
  SignatureV4Service,
} from '@storage/protocols/s3/signature-v4'
import { createRawSignatureV4Signer } from '../../../test/utils/signature-v4'

const credentials = {
  accessKeyId: 'access-key',
  secretAccessKey: 'secret-key',
  region: 'us-east-1',
  service: SignatureV4Service.S3,
}

const verifier = new SignatureV4({
  enforceRegion: false,
  credentials: {
    accessKey: credentials.accessKeyId,
    secretKey: credentials.secretAccessKey,
    region: credentials.region,
    service: credentials.service,
  },
})

type SignedRequest = {
  method: string
  path: string
  query: Record<string, string>
  headers: Record<string, string>
}

type AwsRequest = Omit<SignedRequest, 'query'> & {
  query?: Record<string, string | string[] | null>
}

const forwardedPrefix = '/storage/v1'

async function signWithAwsClient(key: string, versionId?: string) {
  let signedRequest: SignedRequest | undefined
  const client = new S3Client({
    endpoint: `http://storage.test${forwardedPrefix}`,
    forcePathStyle: true,
    region: credentials.region,
    credentials,
    requestHandler: {
      handle: async (request: AwsRequest) => {
        signedRequest = {
          method: request.method,
          path: request.path,
          query: Object.fromEntries(
            Object.entries(request.query ?? {}).map(([key, value]) => [
              key,
              Array.isArray(value) ? value.join(',') : (value ?? ''),
            ])
          ),
          headers: request.headers,
        }

        return {
          response: {
            statusCode: 200,
            headers: {},
          },
        }
      },
    },
  })

  try {
    await client.send(
      new HeadObjectCommand({
        Bucket: 'bucket',
        Key: key,
        VersionId: versionId,
      })
    )
  } finally {
    client.destroy()
  }

  if (!signedRequest) {
    throw new Error('AWS client did not produce a signed request')
  }

  return signedRequest
}

function requestTarget(request: SignedRequest) {
  const path = request.path.slice(forwardedPrefix.length)
  const query = Object.entries(request.query)
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&')

  return query ? `${path}?${query}` : path
}

const signRawPath = createRawSignatureV4Signer(credentials)

function definedHeaders(headers: IncomingHttpHeaders) {
  const result: Record<string, string | string[]> = {}
  for (const [name, value] of Object.entries(headers)) {
    if (value !== undefined) {
      result[name] = value
    }
  }
  return result
}

describe('SignatureV4 verification', () => {
  it.each([
    {
      name: 'parent segment without a query string',
      key: 'folder/../object',
      prefix: forwardedPrefix,
      versionId: undefined,
    },
    {
      name: 'parent segment with a query string and trailing-slash prefix',
      key: 'folder/../object',
      prefix: `${forwardedPrefix}/`,
      versionId: 'version-1',
    },
    {
      name: 'current segment without a query string',
      key: 'folder/./object',
      prefix: forwardedPrefix,
      versionId: undefined,
    },
  ])('verifies an AWS-signed dot-segment path with a $name', async ({ key, prefix, versionId }) => {
    const signedRequest = await signWithAwsClient(key, versionId)
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      verifier.verify(clientSignature, {
        url: requestTarget(signedRequest),
        prefix,
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: signedRequest.query,
      })
    ).resolves.toBe(true)
  })

  it.each([
    {
      name: 'literal parent segment',
      parentSegment: '..',
    },
    {
      name: 'uppercase encoded parent segment',
      parentSegment: '%2E%2E',
    },
    {
      name: 'lowercase encoded parent segment',
      parentSegment: '%2e%2e',
    },
    {
      name: 'literal dot followed by an encoded dot',
      parentSegment: '.%2E',
    },
    {
      name: 'encoded dot followed by a literal dot',
      parentSegment: '%2E.',
    },
  ])('binds signatures to the raw path for a $name', async ({ parentSegment }) => {
    const requestPath = `/bucket/folder/${parentSegment}/object`
    const exactRequest = await signRawPath(`${forwardedPrefix}${requestPath}`)
    const exactSignature = SignatureV4.parseAuthorizationHeader(exactRequest.headers)

    await expect(
      verifier.verify(exactSignature, {
        url: requestPath,
        prefix: forwardedPrefix,
        headers: exactRequest.headers,
        method: exactRequest.method,
        query: {},
      })
    ).resolves.toBe(true)

    const normalizedRequest = await signRawPath(`${forwardedPrefix}/bucket/object`)
    const normalizedSignature = SignatureV4.parseAuthorizationHeader(normalizedRequest.headers)

    await expect(
      verifier.verify(normalizedSignature, {
        url: requestPath,
        prefix: forwardedPrefix,
        headers: normalizedRequest.headers,
        method: normalizedRequest.method,
        query: {},
      })
    ).resolves.toBe(false)
  })

  it('rejects a wrong-length signature instead of throwing', async () => {
    const signedRequest = await signWithAwsClient('object')
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      verifier.verify(
        { ...clientSignature, signature: 'abc' },
        {
          url: requestTarget(signedRequest),
          prefix: forwardedPrefix,
          headers: signedRequest.headers,
          method: signedRequest.method,
          query: signedRequest.query,
        }
      )
    ).resolves.toBe(false)
  })

  it('rejects a wrong-length POST policy signature instead of throwing', () => {
    const policy = Buffer.from(
      JSON.stringify({ expiration: '2030-01-01T00:00:00Z', conditions: [] })
    ).toString('base64')

    expect(
      verifier.verifyPostPolicySignature(
        {
          credentials: {
            accessKey: credentials.accessKeyId,
            shortDate: '20260818',
            region: credentials.region,
            service: credentials.service,
          },
          signature: 'abc',
          signedHeaders: [],
          longDate: '20260818T000000Z',
        },
        policy
      )
    ).toBe(false)
  })

  it('verifies a raw percent-encoded parent segment over HTTP', async () => {
    const rawPath = `${forwardedPrefix}/bucket/folder/%2E%2E/object`
    const signedRequest = await signRawPath(rawPath)
    let receivedTarget: string | undefined
    let verificationResult = false
    let verificationError: unknown

    const server = createServer((request, response) => {
      void (async () => {
        receivedTarget = request.url
        const headers = definedHeaders(request.headers)
        const clientSignature = SignatureV4.parseAuthorizationHeader(headers)
        const storageRequestTarget = (request.url ?? '').slice(forwardedPrefix.length)

        verificationResult = await verifier.verify(clientSignature, {
          url: storageRequestTarget,
          prefix: forwardedPrefix,
          headers,
          method: request.method ?? 'HEAD',
          query: {},
        })

        response.writeHead(verificationResult ? 204 : 403)
        response.end()
      })().catch((error: unknown) => {
        verificationError = error
        response.writeHead(500)
        response.end()
      })
    })

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve)
    })

    try {
      const address = server.address() as AddressInfo
      const statusCode = await new Promise<number | undefined>((resolve, reject) => {
        const request = httpRequest(
          {
            host: '127.0.0.1',
            port: address.port,
            method: signedRequest.method,
            path: signedRequest.path,
            headers: signedRequest.headers,
          },
          (response) => {
            response.resume()
            response.on('end', () => resolve(response.statusCode))
          }
        )
        request.on('error', reject)
        request.end()
      })

      if (verificationError) {
        throw verificationError
      }
      expect(receivedTarget).toBe(rawPath)
      expect(verificationResult).toBe(true)
      expect(statusCode).toBe(204)
    } finally {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error)
          } else {
            resolve()
          }
        })
      })
    }
  })

  it('verifies a root-path signature for an empty request target', async () => {
    const signedRequest = await signRawPath('/')
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      verifier.verify(clientSignature, {
        url: '',
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: {},
      })
    ).resolves.toBe(true)
  })
})

describe('SignatureV4 multiple secret candidates', () => {
  const fallbackSecret = 'fallback-secret-key'

  function multiSecretVerifier() {
    return new SignatureV4({
      enforceRegion: false,
      credentials: {
        accessKey: credentials.accessKeyId,
        secretKey: [credentials.secretAccessKey, fallbackSecret],
        region: credentials.region,
        service: credentials.service,
      },
    })
  }

  function deriveSigningKey(secret: string, shortDate: string, region: string, service: string) {
    const kDate = createHmac('sha256', `AWS4${secret}`).update(shortDate).digest()
    const kRegion = createHmac('sha256', kDate).update(region).digest()
    const kService = createHmac('sha256', kRegion).update(service).digest()
    return createHmac('sha256', kService).update('aws4_request').digest()
  }

  const signWithFallbackSecret = createRawSignatureV4Signer({
    ...credentials,
    secretAccessKey: fallbackSecret,
  })

  it.each([
    { name: 'primary', sign: signRawPath },
    { name: 'fallback', sign: signWithFallbackSecret },
  ])('verifies a request signed with the $name secret', async ({ sign }) => {
    const signedRequest = await sign('/bucket/object')
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      multiSecretVerifier().verify(clientSignature, {
        url: '/bucket/object',
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: {},
      })
    ).resolves.toBe(true)
  })

  it('rejects a request signed with an unknown secret', async () => {
    const signWithUnknownSecret = createRawSignatureV4Signer({
      ...credentials,
      secretAccessKey: 'unknown-secret-key',
    })
    const signedRequest = await signWithUnknownSecret('/bucket/object')
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      multiSecretVerifier().verify(clientSignature, {
        url: '/bucket/object',
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: {},
      })
    ).resolves.toBe(false)
  })

  it('validates chunk signatures with the secret that matched the seed signature', async () => {
    const signedRequest = await signWithFallbackSecret('/bucket/object')
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)
    const chunkVerifier = multiSecretVerifier()

    await expect(
      chunkVerifier.verify(clientSignature, {
        url: '/bucket/object',
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: {},
      })
    ).resolves.toBe(true)

    const { shortDate, region, service } = clientSignature.credentials
    const chunkHash = createHash('sha256').update('chunk-data').digest('hex')
    const stringToSign = [
      'AWS4-HMAC-SHA256-PAYLOAD',
      clientSignature.longDate,
      `${shortDate}/${region}/${service}/aws4_request`,
      clientSignature.signature,
      EMPTY_SHA256_HASH,
      chunkHash,
    ].join('\n')

    const chunkSignature = (secret: string) =>
      createHmac('sha256', deriveSigningKey(secret, shortDate, region, service))
        .update(stringToSign)
        .digest('hex')

    expect(
      chunkVerifier.validateChunkSignature(clientSignature, chunkHash, chunkSignature(fallbackSecret))
    ).toBe(true)
    expect(
      chunkVerifier.validateChunkSignature(
        clientSignature,
        chunkHash,
        chunkSignature(credentials.secretAccessKey)
      )
    ).toBe(false)
  })

  it('verifies a POST policy signed with the fallback secret', () => {
    const policy = Buffer.from(
      JSON.stringify({ expiration: '2030-01-01T00:00:00Z', conditions: [] })
    ).toString('base64')
    const shortDate = '20260818'

    const signature = createHmac(
      'sha256',
      deriveSigningKey(fallbackSecret, shortDate, credentials.region, credentials.service)
    )
      .update(policy)
      .digest('hex')

    expect(
      multiSecretVerifier().verifyPostPolicySignature(
        {
          credentials: {
            accessKey: credentials.accessKeyId,
            shortDate,
            region: credentials.region,
            service: credentials.service,
          },
          signature,
          signedHeaders: [],
          longDate: `${shortDate}T000000Z`,
        },
        policy
      )
    ).toBe(true)
  })
})
