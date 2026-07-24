import {
  JWK_KID_SEPARATOR,
  JWK_KIND_STORAGE_URL_SIGNING,
  JWK_KIND_STORAGE_URL_STANDBY,
} from '@internal/auth/jwks'
import { UrlSigningJwkGenerator } from '@internal/auth/jwks/generator'
import { jwksManager } from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { JwksRollUrlSigningKey } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig, URL_SIGNING_JWK_TYPES } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { urlSigningJwkType } = getConfig()

const addSchema = {
  body: {
    type: 'object',
    properties: {
      jwk: {
        type: 'object',
        properties: {
          kty: { type: 'string' },
        },
        required: ['kty'],
      },
      kind: {
        type: 'string',
      },
    },
    required: ['jwk', 'kind'],
  },
} as const

const updateSchema = {
  body: {
    type: 'object',
    properties: {
      active: {
        type: 'boolean',
      },
    },
    required: ['active'],
  },
} as const

const generateStandbySchema = {
  body: {
    type: 'object',
    properties: {
      type: {
        type: 'string',
        enum: URL_SIGNING_JWK_TYPES,
      },
    },
    required: ['type'],
  },
} as const

const rollSchema = {
  body: {
    type: 'object',
    properties: {
      type: {
        type: 'string',
        enum: URL_SIGNING_JWK_TYPES,
      },
    },
    required: ['type'],
  },
} as const

interface JwksAddRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof addSchema.body>
  Params: {
    tenantId: string
  }
}

interface JwksUpdateRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof updateSchema.body>
  Params: {
    tenantId: string
    kid: string
  }
}

interface JwksRollRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof rollSchema.body>
  Params: {
    tenantId: string
  }
}

interface JwksListRequestInterface extends RequestGenericInterface {
  Params: {
    tenantId: string
  }
}

interface JwksGenerateStandbyRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof generateStandbySchema.body>
  Params: {
    tenantId: string
  }
}

interface JwksSwapStandbyRequestInterface extends RequestGenericInterface {
  Params: {
    tenantId: string
    kid: string
  }
}

type ValidationResult = { message: string } | undefined

function validateAddJwkRequest({ jwk, kind }: JwksAddRequestInterface['Body']): ValidationResult {
  if (kind.includes(JWK_KID_SEPARATOR)) {
    return { message: `Kind cannot contain restricted character "${JWK_KID_SEPARATOR}"` }
  }

  if (kind.length > 50) {
    return { message: 'Kind cannot exceed 50 characters' }
  }

  if (kind === JWK_KIND_STORAGE_URL_SIGNING || kind === JWK_KIND_STORAGE_URL_STANDBY) {
    return { message: `Cannot create add a jwk using reserved kind "${kind}"` }
  }

  switch (jwk.kty) {
    case 'oct':
      if (!jwk.k) {
        return { message: 'Invalid symmetric jwk. k is required' }
      }
      break
    case 'RSA':
      if (!jwk.n || !jwk.e) {
        return { message: 'Invalid asymmetric jwk. RSA must include n and e' }
      }
      if (jwk.d || jwk.p || jwk.q || jwk.dp || jwk.dq || jwk.qi) {
        return { message: 'Invalid asymmetric public jwk. Private fields are not allowed' }
      }
      break
    case 'EC':
      if (!jwk.crv || !jwk.x || !jwk.y) {
        return { message: 'Invalid asymmetric jwk. EC must include crv, x, and y' }
      }
      if (jwk.d) {
        return { message: 'Invalid asymmetric public jwk. Private fields are not allowed' }
      }
      break
    case 'OKP':
      if (!jwk.crv || !jwk.x) {
        return { message: 'Invalid asymmetric jwk. OKP must include crv and x' }
      }
      if (jwk.d) {
        return { message: 'Invalid asymmetric public jwk. Private fields are not allowed' }
      }
      break
    default:
      return { message: 'Unsupported jwk algorithm ' + jwk.kty }
  }
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  fastify.post<JwksAddRequestInterface>(
    '/:tenantId/jwks',
    { schema: { ...addSchema, tags: ['jwks'] } },
    async ({ body, params }, reply) => {
      const validationResult = validateAddJwkRequest(body)
      if (validationResult?.message) {
        return reply.status(400).send(validationResult.message)
      }

      const result = await jwksManager.addJwk(params.tenantId, body.jwk, body.kind)
      return reply.status(201).send(result)
    }
  )

  fastify.put<JwksUpdateRequestInterface>(
    '/:tenantId/jwks/:kid',
    { schema: { ...updateSchema, tags: ['jwks'] } },
    async (request, reply) => {
      const {
        params: { tenantId, kid },
        body: { active },
      } = request
      const result = await jwksManager.toggleJwkActive(tenantId, kid, active)
      return reply.send({ result })
    }
  )

  fastify.get<JwksListRequestInterface>(
    '/:tenantId/jwks',
    { schema: { tags: ['jwks'] } },
    async (request, reply) => {
      const { tenantId } = request.params

      const result = await jwksManager.listJwks(tenantId)
      return reply.send(result)
    }
  )

  fastify.post<JwksGenerateStandbyRequestInterface>(
    '/:tenantId/jwks/url-signing/standby',
    { schema: { ...generateStandbySchema, tags: ['jwks'] } },
    async (request, reply) => {
      const {
        params: { tenantId },
        body: { type },
      } = request

      const result = await jwksManager.generateUrlSigningStandbyJwk(tenantId, type)
      return reply.status(201).send(result)
    }
  )

  fastify.post<JwksSwapStandbyRequestInterface>(
    '/:tenantId/jwks/url-signing/standby/:kid/swap',
    { schema: { tags: ['jwks'] } },
    async (request, reply) => {
      const { tenantId, kid } = request.params

      const swapped = await jwksManager.swapUrlSigningStandbyJwk(tenantId, kid)
      if (!swapped) {
        return reply.status(404).send({ error: 'Standby jwk not found' })
      }
      return reply.status(201).send()
    }
  )

  fastify.post<JwksRollRequestInterface>(
    '/:tenantId/jwks/url-signing/roll',
    { schema: { ...rollSchema, tags: ['jwks'] } },
    async (request, reply) => {
      const {
        params: { tenantId },
        body: { type },
      } = request

      await JwksRollUrlSigningKey.send({
        tenantId,
        tenant: {
          ref: tenantId,
          host: '',
        },
        keyType: type,
        sbReqId: request.sbReqId,
      })

      return reply.send({ started: true })
    }
  )

  fastify.post(
    '/jwks/generate-all-missing',
    { schema: { tags: ['jwks'] } },
    async (request, reply) => {
      const { running, sent } = UrlSigningJwkGenerator.getGenerationStatus()
      if (running) {
        return reply
          .status(400)
          .send(`Generate missing jwks is already running, and has sent ${sent} items so far`)
      }

      UrlSigningJwkGenerator.generateUrlSigningJwksOnAllTenants({
        keyType: urlSigningJwkType,
        sbReqId: request.sbReqId,
        signal: request.signals.disconnect.signal,
      }).catch((e) => {
        logSchema.error(request.log, 'Error generating url signing jwks for all tenants', {
          type: 'jwk-generator',
          tenantId: request.tenantId,
          project: request.tenantId,
          reqId: request.id,
          sbReqId: request.sbReqId,
          error: e,
        })
      })
      return reply.send({ started: true })
    }
  )

  fastify.get('/jwks/generate-all-missing', { schema: { tags: ['jwks'] } }, (request, reply) => {
    return reply.send(UrlSigningJwkGenerator.getGenerationStatus())
  })
}
