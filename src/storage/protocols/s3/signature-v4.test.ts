import { createServer, request as httpRequest, type IncomingHttpHeaders } from 'node:http'
import { type AddressInfo } from 'node:net'
import { HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { SignatureV4, SignatureV4Service } from '@storage/protocols/s3/signature-v4'
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

async function signWithAwsClient(command: HeadObjectCommand | PutObjectCommand) {
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
    await client.send(command)
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
    'annual  report',
    '  annual report  ',
    'annual\t \treport',
    'annual report',
  ])('verifies an AWS-signed upload with metadata %j', async (description) => {
    const signedRequest = await signWithAwsClient(
      new PutObjectCommand({
        Bucket: 'bucket',
        Key: 'object',
        Body: 'content',
        Metadata: { description },
      })
    )
    const clientSignature = SignatureV4.parseAuthorizationHeader(signedRequest.headers)

    await expect(
      verifier.verify(clientSignature, {
        url: requestTarget(signedRequest),
        prefix: forwardedPrefix,
        headers: signedRequest.headers,
        method: signedRequest.method,
        query: signedRequest.query,
      })
    ).resolves.toBe(true)

    expect(signedRequest.headers['x-amz-meta-description']).toBe(description)
    await expect(
      verifier.verify(clientSignature, {
        url: requestTarget(signedRequest),
        prefix: forwardedPrefix,
        headers: { ...signedRequest.headers, 'x-amz-meta-description': 'different report' },
        method: signedRequest.method,
        query: signedRequest.query,
      })
    ).resolves.toBe(false)
  })

  it('verifies AWS SDK metadata whitespace over HTTP without changing the received value', async () => {
    const description = 'annual  report'
    let receivedDescription: string | string[] | undefined
    let verificationError: unknown
    const server = createServer((request, response) => {
      void (async () => {
        const headers = definedHeaders(request.headers)
        receivedDescription = headers['x-amz-meta-description']
        const target = new URL(request.url ?? '/', 'http://localhost')
        const isVerified = await verifier.verify(SignatureV4.parseAuthorizationHeader(headers), {
          url: (request.url ?? '').slice(forwardedPrefix.length),
          prefix: forwardedPrefix,
          headers,
          method: request.method ?? 'PUT',
          query: Object.fromEntries(target.searchParams),
          body: request,
        })
        request.resume()
        response.writeHead(isVerified ? 200 : 403)
        response.end()
      })().catch((error: unknown) => {
        verificationError = error
        response.writeHead(500)
        response.end()
      })
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))

    const address = server.address() as AddressInfo
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${address.port}${forwardedPrefix}`,
      forcePathStyle: true,
      region: credentials.region,
      credentials,
      maxAttempts: 1,
    })
    try {
      const result = await client.send(
        new PutObjectCommand({
          Bucket: 'bucket',
          Key: 'object',
          Body: 'content',
          Metadata: { description },
        })
      )
      expect(verificationError).toBeUndefined()
      expect(result.$metadata.httpStatusCode).toBe(200)
      expect(receivedDescription).toBe(description)
    } finally {
      client.destroy()
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()))
      })
    }
  })

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
    const signedRequest = await signWithAwsClient(
      new HeadObjectCommand({ Bucket: 'bucket', Key: key, VersionId: versionId })
    )
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
    const signedRequest = await signWithAwsClient(
      new HeadObjectCommand({ Bucket: 'bucket', Key: 'object' })
    )
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
