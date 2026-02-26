import { UrlSigningJwkGenerator } from '@internal/auth/jwks/generator'
import { jwksManager } from '@internal/database'
import { logSchema } from '@internal/monitoring'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'

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

type ValidationResult = { message: string } | undefined

function validateAddJwkRequest({ jwk, kind }: JwksAddRequestInterface['Body']): ValidationResult {
  if (kind.includes('_')) {
    return { message: 'Kind cannot contain underscore characters' }
  }

  if (kind.length > 50) {
    return { message: 'Kind cannot exceed 50 characters' }
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
  fastify.register(apiKey)

  fastify.post<JwksAddRequestInterface>(
    '/:tenantId/jwks',
    { schema: addSchema },
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
    { schema: updateSchema },
    async (request, reply) => {
      const {
        params: { tenantId, kid },
        body: { active },
      } = request
      const result = await jwksManager.toggleJwkActive(tenantId, kid, active)
      return reply.send({ result })
    }
  )

  fastify.post('/jwks/generate-all-missing', async (request, reply) => {
    const { running, sent } = UrlSigningJwkGenerator.getGenerationStatus()
    if (running) {
      return reply
        .status(400)
        .send(`Generate missing jwks is already running, and has sent ${sent} items so far`)
    }

    UrlSigningJwkGenerator.generateUrlSigningJwksOnAllTenants(
      request.signals.disconnect.signal
    ).catch((e) => {
      logSchema.error(request.log, 'Error generating url signing jwks for all tenants', {
        type: 'jwk-generator',
        error: e,
      })
    })
    return reply.send({ started: true })
  })

  fastify.get('/jwks/generate-all-missing', (request, reply) => {
    return reply.send(UrlSigningJwkGenerator.getGenerationStatus())
  })
}
