import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { signJWT, VERCEL_OIDC_ISSUER_PREFIX } from '@internal/auth'
import Fastify from 'fastify'
import { vi } from 'vitest'
import { createRawSignatureV4Signer } from '../../test/utils/signature-v4'
import { s3ErrorHandler } from '../routes/s3/error-handler'
import { signatureV4 } from './signature-v4'
import { xmlParser } from './xml'

const { getJwtSecretMock, getTenantConfigMock } = vi.hoisted(() => ({
  getJwtSecretMock: vi.fn(),
  getTenantConfigMock: vi.fn(),
}))

vi.mock('../../config', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../config')>()
  const config = {
    ...actual.getConfig(),
    anonKeyAsync: Promise.resolve('anon-key'),
    isMultitenant: false,
    requestAllowXForwardedPrefix: true,
    s3ProtocolAllowForwardedHeader: false,
    s3ProtocolEnforceRegion: false,
    s3ProtocolNonCanonicalHostHeader: undefined,
    s3ProtocolPrefix: '',
    storagePublicUrl: undefined,
    storageS3Region: 'us-east-1',
  }
  return { ...actual, getConfig: () => config }
})

vi.mock('@internal/database', () => ({
  getJwtSecret: getJwtSecretMock,
  getTenantConfig: getTenantConfigMock,
  s3CredentialsManager: {
    getS3CredentialsByAccessKey: vi.fn(),
  },
}))

const tenantId = 'tenant-ref'
const jwtSecret = 'session-token-test-secret-0123456789abcdef'
const forwardedPrefix = '/storage/v1'
const internalPath = '/bucket/object'
const vercelIssuer = `${VERCEL_OIDC_ISSUER_PREFIX}oidc/team-id`

async function buildApp() {
  getJwtSecretMock.mockResolvedValue({ secret: jwtSecret, jwks: null })

  const app = Fastify()
  let owner: string | undefined
  let handled = false

  app.decorateRequest('tenantId', tenantId)
  app.setErrorHandler(s3ErrorHandler)
  app.register(xmlParser, {
    disableContentParser: true,
    responseNamespace: 'http://s3.amazonaws.com/doc/2006-03-01/',
  })
  app.register(signatureV4, { allowBodyHash: false })
  app.get('/:Bucket/*', async (request, reply) => {
    handled = true
    owner = request.owner
    return reply.status(204).send()
  })
  await app.ready()

  return { app, owner: () => owner, wasHandled: () => handled }
}

function createVercelToken(sub: string) {
  return signJWT({ iss: vercelIssuer, sub, role: 'service_role' }, jwtSecret, 60)
}

const credentials = (sessionToken: string) => ({
  accessKeyId: tenantId,
  secretAccessKey: 'anon-key',
  sessionToken,
  region: 'us-east-1',
  service: 's3',
})

async function sendWithHeaderToken(
  app: Awaited<ReturnType<typeof buildApp>>['app'],
  token: string
) {
  const signedRequest = await createRawSignatureV4Signer(credentials(token), { method: 'GET' })(
    `${forwardedPrefix}${internalPath}`
  )

  return app.inject({
    method: 'GET',
    url: internalPath,
    headers: { ...signedRequest.headers, 'x-forwarded-prefix': forwardedPrefix },
  })
}

async function sendWithQueryToken(app: Awaited<ReturnType<typeof buildApp>>['app'], token: string) {
  const client = new S3Client({
    endpoint: `http://storage.test${forwardedPrefix}`,
    forcePathStyle: true,
    region: 'us-east-1',
    credentials: credentials(token),
  })

  try {
    const url = new URL(
      await getSignedUrl(client, new GetObjectCommand({ Bucket: 'bucket', Key: 'object' }), {
        expiresIn: 60,
      })
    )
    expect(url.searchParams.get('X-Amz-Security-Token')).toBe(token)

    return await app.inject({
      method: 'GET',
      url: `${url.pathname.slice(forwardedPrefix.length)}${url.search}`,
      headers: { host: url.host, 'x-forwarded-prefix': forwardedPrefix },
    })
  } finally {
    client.destroy()
  }
}

describe.each([
  { location: 'header', send: sendWithHeaderToken },
  { location: 'query', send: sendWithQueryToken },
])('SignatureV4 session token in the $location with real JWT verification', ({ send }) => {
  it('accepts a Vercel token whose subject is the tenant', async () => {
    const { app, owner, wasHandled } = await buildApp()

    try {
      const response = await send(app, await createVercelToken(tenantId))

      expect(response.statusCode, response.body).toBe(204)
      expect(wasHandled()).toBe(true)
      expect(owner()).toBe(tenantId)
    } finally {
      await app.close()
    }
  })

  it('rejects a Vercel token whose subject is another tenant', async () => {
    const { app, wasHandled } = await buildApp()

    try {
      const response = await send(app, await createVercelToken('other-tenant-ref'))

      expect(response.statusCode).toBe(403)
      expect(response.body).toContain('<Code>AccessDenied</Code>')
      expect(wasHandled()).toBe(false)
    } finally {
      await app.close()
    }
  })
})
