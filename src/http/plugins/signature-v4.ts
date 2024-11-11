import { FastifyInstance, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getS3CredentialsByAccessKey, getTenantConfig } from '@internal/database'
import { ClientSignature, SignatureV4 } from '@storage/protocols/s3'
import { signJWT, verifyJWT } from '@internal/auth'
import { ERRORS } from '@internal/errors'

import { getConfig } from '../../config'

const {
  anonKey,
  jwtSecret,
  jwtJWKS,
  serviceKey,
  storageS3Region,
  isMultitenant,
  requestAllowXForwardedPrefix,
  s3ProtocolPrefix,
  s3ProtocolAllowForwardedHeader,
  s3ProtocolEnforceRegion,
  s3ProtocolAccessKeyId,
  s3ProtocolAccessKeySecret,
  s3ProtocolNonCanonicalHostHeader,
} = getConfig()

type AWSRequest = FastifyRequest<{ Querystring: { 'X-Amz-Credential'?: string } }>

export const signatureV4 = fastifyPlugin(
  async function (fastify: FastifyInstance) {
    fastify.addHook('preHandler', async (request: AWSRequest) => {
      const clientSignature = extractSignature(request)

      const sessionToken = clientSignature.sessionToken

      const {
        signature: signatureV4,
        claims,
        token,
      } = await createServerSignature(request.tenantId, clientSignature)

      let storagePrefix = s3ProtocolPrefix
      if (
        requestAllowXForwardedPrefix &&
        typeof request.headers['x-forwarded-prefix'] === 'string'
      ) {
        storagePrefix = request.headers['x-forwarded-prefix']
      }

      const isVerified = signatureV4.verify(clientSignature, {
        url: request.url,
        body: request.body as string | ReadableStream | Buffer,
        headers: request.headers as Record<string, string | string[]>,
        method: request.method,
        query: request.query as Record<string, string>,
        prefix: storagePrefix,
      })

      if (!isVerified && !sessionToken) {
        throw ERRORS.SignatureDoesNotMatch(
          'The request signature we calculated does not match the signature you provided. Check your key and signing method.'
        )
      }

      if (!isVerified && sessionToken) {
        throw ERRORS.SignatureDoesNotMatch(
          'The request signature we calculated does not match the signature you provided, Check your credentials. ' +
            'The session token should be a valid JWT token'
        )
      }

      const jwtSecrets = {
        jwtSecret: jwtSecret,
        jwks: jwtJWKS,
      }

      if (isMultitenant) {
        const tenant = await getTenantConfig(request.tenantId)
        jwtSecrets.jwtSecret = tenant.jwtSecret
        jwtSecrets.jwks = tenant.jwks || undefined
      }

      if (token) {
        const payload = await verifyJWT(token, jwtSecrets.jwtSecret, jwtSecrets.jwks)
        request.jwt = token
        request.jwtPayload = payload
        request.owner = payload.sub
        return
      }

      if (!claims) {
        throw ERRORS.AccessDenied('Missing claims')
      }

      const jwt = await signJWT(claims, jwtSecrets.jwtSecret, '5m')

      request.jwt = jwt
      request.jwtPayload = claims
      request.owner = claims.sub
    })
  },
  { name: 'auth-signature-v4' }
)

function extractSignature(req: AWSRequest) {
  if (typeof req.headers.authorization === 'string') {
    return SignatureV4.parseAuthorizationHeader(req.headers)
  }

  if (typeof req.query['X-Amz-Credential'] === 'string') {
    return SignatureV4.parseQuerySignature(req.query)
  }

  throw ERRORS.AccessDenied('Missing signature')
}

async function createServerSignature(tenantId: string, clientSignature: ClientSignature) {
  const awsRegion = storageS3Region
  const awsService = 's3'

  if (clientSignature?.sessionToken) {
    const tenantAnonKey = isMultitenant ? (await getTenantConfig(tenantId)).anonKey : anonKey

    if (!tenantAnonKey) {
      throw ERRORS.AccessDenied('Missing tenant anon key')
    }

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      allowForwardedHeader: s3ProtocolAllowForwardedHeader,
      nonCanonicalForwardedHost: s3ProtocolNonCanonicalHostHeader,
      credentials: {
        accessKey: tenantId,
        secretKey: tenantAnonKey,
        region: awsRegion,
        service: awsService,
      },
    })

    return { signature, claims: undefined, token: clientSignature.sessionToken }
  }

  if (isMultitenant) {
    const credential = await getS3CredentialsByAccessKey(
      tenantId,
      clientSignature.credentials.accessKey
    )

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      allowForwardedHeader: s3ProtocolAllowForwardedHeader,
      nonCanonicalForwardedHost: s3ProtocolNonCanonicalHostHeader,
      credentials: {
        accessKey: credential.accessKey,
        secretKey: credential.secretKey,
        region: awsRegion,
        service: awsService,
      },
    })

    return { signature, claims: credential.claims, token: undefined }
  }

  if (!s3ProtocolAccessKeyId || !s3ProtocolAccessKeySecret) {
    throw ERRORS.AccessDenied(
      'Missing S3 Protocol Access Key ID or Secret Key Environment variables'
    )
  }

  const signature = new SignatureV4({
    enforceRegion: s3ProtocolEnforceRegion,
    allowForwardedHeader: s3ProtocolAllowForwardedHeader,
    nonCanonicalForwardedHost: s3ProtocolNonCanonicalHostHeader,
    credentials: {
      accessKey: s3ProtocolAccessKeyId,
      secretKey: s3ProtocolAccessKeySecret,
      region: awsRegion,
      service: awsService,
    },
  })

  return { signature, claims: undefined, token: serviceKey }
}
