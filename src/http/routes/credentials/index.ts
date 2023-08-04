import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { db, jwtServiceKey, storage } from '../../plugins'

const postSchema = {
  body: {
    type: 'object',
    properties: {
      name: { type: 'string' },
      access_key: { type: 'string' },
      secret_key: { type: 'string' },
      role: { type: 'string' },
      endpoint: { type: 'string' },
      region: {
        type: 'string',
        // oneOf: ['us-east-2', 'us-east-1', 'us-west-1', 'us-west-2', 'af-south-1'], // TODO: add more
      },
    },
    required: ['name', 'provider', 'region'],
  },
} as const

interface createCredentialsSchema extends RequestGenericInterface {
  Body: FromSchema<typeof postSchema.body>
}

interface deleteCredentialsSchema extends RequestGenericInterface {
  Params: {
    credentialId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(jwtServiceKey)
  fastify.register(db)
  fastify.register(storage)

  fastify.get('/', async (request, reply) => {
    const credentials = await request.storage.listCredentials()

    reply.status(200).send(credentials)
  })

  fastify.post<createCredentialsSchema>('/', async (request, reply) => {
    const { name, access_key, secret_key, role, endpoint, region, force_path_style } = request.body

    const credential = await request.storage.createCredential({
      name,
      access_key,
      secret_key,
      role,
      endpoint,
      region,
      force_path_style: Boolean(force_path_style),
    })

    reply.status(201).send(credential)
  })

  fastify.delete<deleteCredentialsSchema>('/:credentialId', async (request, reply) => {
    await request.storage.deleteCredential(request.params.credentialId)

    reply.code(204).send()
  })
}
