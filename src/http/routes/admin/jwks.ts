import { FastifyInstance, RequestGenericInterface } from 'fastify'
import apiKey from '../../plugins/apikey'
import { addJwk, toggleJwkActive } from '@internal/database'
import { FromSchema } from 'json-schema-to-ts'
import { UrlSigningJwkGenerator } from '@internal/database/url-signing-jwk-generator'

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

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post<JwksAddRequestInterface>(
    '/:tenantId/jwks',
    { schema: addSchema },
    async (request, reply) => {
      const {
        params: { tenantId },
        body: { jwk, kind },
      } = request

      if (kind.includes('_')) {
        return reply.status(400).send('Kind cannot contain underscore characters')
      }

      if (kind.length > 50) {
        return reply.status(400).send('Kind cannot exceed 50 characters')
      }

      switch (jwk.kty) {
        case 'oct':
          if (!jwk.k) {
            return reply.status(400).send('Invalid symmetric jwk. k is required')
          }
          break
        case 'RSA':
          if (!jwk.n || !jwk.e) {
            return reply.status(400).send('Invalid asymmetric jwk. RSA must include n and e')
          }
          if (jwk.d || jwk.p || jwk.q || jwk.dp || jwk.dq || jwk.qi) {
            return reply
              .status(400)
              .send('Invalid asymmetric public jwk. Private fields are not allowed')
          }
          break
        case 'EC':
          if (!jwk.crv || !jwk.x || !jwk.y) {
            return reply.status(400).send('Invalid asymmetric jwk. EC must include crv, x, and y')
          }
          if (jwk.d) {
            return reply
              .status(400)
              .send('Invalid asymmetric public jwk. Private fields are not allowed')
          }
          break
        case 'OKP':
          if (!jwk.crv || !jwk.x) {
            return reply.status(400).send('Invalid asymmetric jwk. OKP must include crv and x')
          }
          if (jwk.d) {
            return reply
              .status(400)
              .send('Invalid asymmetric public jwk. Private fields are not allowed')
          }
          // jsonwebtoken does not support OKP (ed25519/Ed448) keys yet, if/when this changes replace this with a break and we should be good to go
          return reply.status(400).send('OKP jwks are not yet supported. Please use RSA or EC')
        default:
          return reply.status(400).send('Unsupported jwk algorithm ' + jwk.kty)
      }

      const result = await addJwk(tenantId, jwk, kind)
      return reply.send(result)
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
      const result = await toggleJwkActive(tenantId, kid, active)
      return reply.send({ result })
    }
  )

  fastify.post('/jwks/generate-all-missing', (request, reply) => {
    const { running, sent } = UrlSigningJwkGenerator.getGenerationStatus()
    if (running) {
      return reply
        .status(400)
        .send(`Generate missing jwks is already running, and has sent ${sent} items so far`)
    }
    UrlSigningJwkGenerator.generateUrlSigningJwksOnAllTenants()
    return reply.send({ started: true })
  })

  fastify.get('/jwks/generate-all-missing', (request, reply) => {
    return reply.send(UrlSigningJwkGenerator.getGenerationStatus())
  })
}
