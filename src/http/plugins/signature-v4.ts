import { FastifyInstance, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getS3CredentialsByAccessKey, getTenantConfig } from '../../database'
import { ClientSignature, SignatureV4 } from '../../storage/protocols/s3'
import { ERRORS } from '../../storage'
import { signJWT, verifyJWT } from '../../auth'
import { getConfig } from '../../config'

const {
  jwtSecret,
  jwtJWKS,
  serviceKey,
  storageS3Region,
  isMultitenant,
  s3ProtocolAllowServiceKeyAsSecret,
  s3ProtocolPrefix,
  s3ProtocolEnforceRegion,
  s3ProtocolAccessKeyId,
  s3ProtocolAccessKeySecret,
} = getConfig()

export const signatureV4 = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (request: FastifyRequest) => {
    const clientCredentials = SignatureV4.parseAuthorizationHeader(
      request.headers.authorization as string
    )

    const sessionToken = request.headers['x-amz-security-token'] as string | undefined

    const {
      signature: signatureV4,
      scopes,
      token,
    } = await createSignature(request.tenantId, clientCredentials, {
      sessionToken: sessionToken,
      allowServiceKeyAsSecret: s3ProtocolAllowServiceKeyAsSecret,
    })

    const isVerified = signatureV4.verify({
      url: request.url,
      body: request.body as string | ReadableStream | Buffer,
      headers: request.headers as Record<string, string | string[]>,
      method: request.method,
      query: request.query as Record<string, string>,
      prefix: s3ProtocolPrefix,
      credentials: clientCredentials.credentials,
      signature: clientCredentials.signature,
      signedHeaders: clientCredentials.signedHeaders,
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

    // it is a session token authentication, we validate the incoming session
    if (sessionToken) {
      const payload = await verifyJWT(sessionToken, jwtSecrets.jwtSecret, jwtSecrets.jwks)
      request.jwt = sessionToken
      request.jwtPayload = payload
      request.owner = payload.sub
      return
    }

    if (token) {
      const payload = await verifyJWT(token, jwtSecrets.jwtSecret, jwtSecrets.jwks)
      request.jwt = token
      request.jwtPayload = payload
      request.owner = payload.sub
      return
    }

    if (!scopes) {
      throw ERRORS.AccessDenied('Missing scopes')
    }

    const jwt = await signJWT(scopes, jwtSecrets.jwtSecret, '5m')

    request.jwt = jwt
    request.jwtPayload = scopes
    request.owner = scopes.sub
  })
})

async function createSignature(
  tenantId: string,
  clientSignature: ClientSignature,
  session?: { sessionToken?: string; allowServiceKeyAsSecret: boolean }
) {
  const awsRegion = storageS3Region
  const awsService = 's3'

  if (session?.sessionToken) {
    const tenant = await getTenantConfig(tenantId)

    if (!tenant.anonKey) {
      throw ERRORS.AccessDenied('Missing tenant anon key')
    }

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      credentials: {
        accessKey: tenantId,
        secretKey: tenant.anonKey,
        region: awsRegion,
        service: awsService,
      },
    })

    return { signature, scopes: undefined }
  }

  if (session?.allowServiceKeyAsSecret && clientSignature.credentials.accessKey === tenantId) {
    const tenantServiceKey = isMultitenant
      ? (await getTenantConfig(tenantId)).serviceKey
      : serviceKey

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      credentials: {
        accessKey: tenantId,
        secretKey: tenantServiceKey,
        region: awsRegion,
        service: awsService,
      },
    })

    return { signature, scopes: undefined, token: tenantServiceKey }
  }

  if (isMultitenant) {
    const credential = await getS3CredentialsByAccessKey(
      tenantId,
      clientSignature.credentials.accessKey
    )

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      credentials: {
        accessKey: credential.accessKey,
        secretKey: credential.secretKey,
        region: awsRegion,
        service: awsService,
      },
    })

    return { signature, scopes: credential.scopes, token: undefined }
  }

  if (!s3ProtocolAccessKeyId || !s3ProtocolAccessKeySecret) {
    throw ERRORS.AccessDenied(
      'Missing S3 Protocol Access Key ID or Secret Key Environment variables'
    )
  }

  const signature = new SignatureV4({
    enforceRegion: s3ProtocolEnforceRegion,
    credentials: {
      accessKey: s3ProtocolAccessKeyId,
      secretKey: s3ProtocolAccessKeySecret,
      region: awsRegion,
      service: awsService,
    },
  })

  return { signature, scopes: undefined, token: serviceKey }
}
