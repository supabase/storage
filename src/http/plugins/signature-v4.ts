import { FastifyInstance, FastifyRequest } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getJwtSecret, getTenantConfig, s3CredentialsManager } from '@internal/database'
import { ClientSignature, SignatureV4, SignatureV4Service } from '@storage/protocols/s3'
import { isJwtToken, signJWT, verifyJWT } from '@internal/auth'
import { ERRORS } from '@internal/errors'

import { getConfig } from '../../config'
import { MultipartFile, MultipartValue } from '@fastify/multipart'
import {
  ChunkSignatureV4Parser,
  V4StreamingAlgorithm,
} from '@storage/protocols/s3/signature-v4-stream'
import { compose, Readable } from 'stream'
import { HashSpillWritable } from '@internal/streams/hash-stream'
import { RequestByteCounterStream } from '@internal/streams'
import { ByteLimitTransformStream } from '@storage/protocols/s3/byte-limit-stream'
import { Writable } from 'node:stream'
import { enforceJwtRole } from './jwt'

const {
  anonKeyAsync,
  serviceKeyAsync,
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

declare module 'fastify' {
  interface FastifyRequest {
    streamingSignatureV4?: ChunkSignatureV4Parser
    multiPartFileStream?: MultipartFile
    bodySha256: string
  }
}

export const signatureV4 = fastifyPlugin(
  async function (
    fastify: FastifyInstance,
    opts: {
      service?: SignatureV4Service
      allowBodyHash?: boolean
      skipIfJwtToken?: boolean
      enforceJwtRoles?: string[]
    } = {
      service: SignatureV4Service.S3,
      allowBodyHash: false,
      skipIfJwtToken: false,
      enforceJwtRoles: [],
    }
  ) {
    // Use preParsing when allowing to pre-calculate the sha256 of the body
    if (opts.allowBodyHash) {
      fastify.addHook('preParsing', async (request: AWSRequest, reply, bodyPayload) => {
        if (opts.skipIfJwtToken && isJwtToken(request.headers.authorization || '')) {
          return bodyPayload
        }

        return await authorizeRequestSignV4(
          request,
          bodyPayload as Readable,
          SignatureV4Service.S3VECTORS,
          opts.allowBodyHash
        )
      })
    }

    // Use preHandler when not allowing to pre-calculate the sha256 of the body
    if (!opts.allowBodyHash) {
      fastify.addHook('preHandler', async (request: AWSRequest) => {
        await authorizeRequestSignV4(request, request.raw as Readable, SignatureV4Service.S3)
      })
    }

    if (opts.enforceJwtRoles) {
      fastify.register(enforceJwtRole, {
        roles: opts.enforceJwtRoles,
      })
    }
  },
  { name: 'auth-signature-v4' }
)

/**
 * Authorize incoming request with Signature V4
 *
 * @param request
 * @param body
 * @param service
 * @param allowBodyHash
 */
async function authorizeRequestSignV4(
  request: AWSRequest,
  body: string | Buffer | Readable,
  service: SignatureV4Service,
  allowBodyHash = false
) {
  const clientSignature = await extractSignature(request)

  const sessionToken = clientSignature.sessionToken

  const {
    signature: signatureV4,
    claims,
    token,
  } = await createServerSignature(request.tenantId, clientSignature, service, allowBodyHash)

  let storagePrefix = s3ProtocolPrefix
  if (requestAllowXForwardedPrefix && typeof request.headers['x-forwarded-prefix'] === 'string') {
    storagePrefix = request.headers['x-forwarded-prefix']
  }

  let hashStreamComposer: (Writable & { digestHex: () => string }) | undefined
  let byteHasherStream:
    | (Writable & {
        digestHex: () => string
        toReadable: (opts: { autoCleanup: boolean }) => Readable
      })
    | undefined

  if (allowBodyHash) {
    byteHasherStream = new HashSpillWritable({
      alg: 'sha256',
      limitInMemoryBytes: 1024 * 1024 * 5, // 5MB
    })
    hashStreamComposer = compose(new ByteLimitTransformStream(1024 * 1024 * 20), byteHasherStream)
    hashStreamComposer!.digestHex = byteHasherStream.digestHex.bind(byteHasherStream)
  }

  const isVerified = await signatureV4.verify(clientSignature, {
    url: request.url,
    body: body,
    headers: request.headers as Record<string, string | string[]>,
    method: request.method,
    query: request.query as Record<string, string>,
    prefix: storagePrefix,
    payloadHasher: hashStreamComposer,
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

  const wasBodyHashed = allowBodyHash && byteHasherStream && byteHasherStream.writableEnded

  const returnStream = wasBodyHashed
    ? byteHasherStream!.toReadable({ autoCleanup: true })
    : (body as Readable)

  const { secret: jwtSecret, jwks } = await getJwtSecret(request.tenantId)

  if (!token) {
    if (!claims) {
      throw ERRORS.AccessDenied('Missing claims')
    }

    const jwt = await signJWT(claims, jwtSecret, '5m')

    request.isAuthenticated = true
    request.jwt = jwt
    request.jwtPayload = claims
    request.owner = claims.sub
  } else {
    const payload = await verifyJWT(token, jwtSecret, jwks)
    request.isAuthenticated = true
    request.jwt = token
    request.jwtPayload = payload
    request.owner = payload.sub
  }

  if (SignatureV4.isChunkedUpload(request.headers)) {
    request.streamingSignatureV4 = createStreamingSignatureV4Parser({
      signatureV4,
      streamAlgorithm: request.headers['x-amz-content-sha256'] as V4StreamingAlgorithm,
      clientSignature,
      trailers: request.headers['x-amz-trailer'] as string,
    })
  }

  if (wasBodyHashed) {
    return compose(returnStream, new RequestByteCounterStream())
  }

  return returnStream
}

async function extractSignature(req: AWSRequest) {
  if (typeof req.headers.authorization === 'string') {
    return SignatureV4.parseAuthorizationHeader(req.headers)
  }

  if (typeof req.query['X-Amz-Credential'] === 'string') {
    return SignatureV4.parseQuerySignature(req.query)
  }

  if (typeof req.isMultipart === 'function' && req.isMultipart()) {
    const formData = new FormData()
    const data = await req.file({
      limits: {
        fields: 20,
        files: 1,
        fileSize: 5 * (1024 * 1024 * 1024),
      },
    })

    const fields = data?.fields
    if (fields) {
      for (const key in fields) {
        const field = fields[key] as MultipartValue<string | Blob>
        if (fields.hasOwnProperty(key) && field.fieldname !== 'file') {
          formData.append(key, field.value)
        }
      }
    }
    // Assign the multipartFileStream for later use
    req.multiPartFileStream = data
    return SignatureV4.parseMultipartSignature(formData)
  }

  throw ERRORS.AccessDenied('Missing signature')
}

async function createServerSignature(
  tenantId: string,
  clientSignature: ClientSignature,
  awsService = SignatureV4Service.S3,
  allowBodyHash = false
) {
  const awsRegion = storageS3Region

  if (clientSignature?.sessionToken) {
    const tenantAnonKey = isMultitenant
      ? (await getTenantConfig(tenantId)).anonKey
      : await anonKeyAsync

    if (!tenantAnonKey) {
      throw ERRORS.AccessDenied('Missing tenant anon key')
    }

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      allowForwardedHeader: s3ProtocolAllowForwardedHeader,
      allowBodyHashing: allowBodyHash,
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
    const credential = await s3CredentialsManager.getS3CredentialsByAccessKey(
      tenantId,
      clientSignature.credentials.accessKey
    )

    const signature = new SignatureV4({
      enforceRegion: s3ProtocolEnforceRegion,
      allowForwardedHeader: s3ProtocolAllowForwardedHeader,
      allowBodyHashing: allowBodyHash,
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
    allowBodyHashing: allowBodyHash,
    nonCanonicalForwardedHost: s3ProtocolNonCanonicalHostHeader,
    credentials: {
      accessKey: s3ProtocolAccessKeyId,
      secretKey: s3ProtocolAccessKeySecret,
      region: awsRegion,
      service: awsService,
    },
  })

  return { signature, claims: undefined, token: await serviceKeyAsync }
}

interface CreateSignatureV3ParserOpts {
  signatureV4: SignatureV4
  streamAlgorithm: string
  clientSignature: ClientSignature
  trailers: string
}

function createStreamingSignatureV4Parser(opts: CreateSignatureV3ParserOpts) {
  const algorithm = opts.streamAlgorithm as V4StreamingAlgorithm
  const trailers = opts.trailers

  const chunkedSignatureV4 = new ChunkSignatureV4Parser({
    maxChunkSize: 8 * 1024 * 1024,
    maxHeaderLength: 256,
    streamingAlgorithm: algorithm,
    trailerHeaderNames: trailers?.split(','),
  })

  chunkedSignatureV4.on(
    'signatureReadyForVerification',
    (signature: string, _: number, hash: string, previousSign) => {
      const isValid = opts.signatureV4.validateChunkSignature(
        opts.clientSignature,
        hash,
        signature,
        previousSign || opts.clientSignature.signature
      )

      if (!isValid) {
        throw ERRORS.SignatureDoesNotMatch(
          'The request signature we calculated does not match the signature you provided. Check your key and signing method.'
        )
      }
    }
  )

  return chunkedSignatureV4
}
