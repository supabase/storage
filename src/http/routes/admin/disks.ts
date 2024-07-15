import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'

import apiKey from '../../plugins/apikey'
import { dbSuperUser, storage } from '../../plugins'

const createDiskSchema = {
  description: 'Create External Disk',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
    },
    required: ['tenantId'],
  },
  body: {
    type: 'object',
    properties: {
      name: { type: 'string', minLength: 3, maxLength: 200 },
      mount_point: { type: 'string' },
      credentials: {
        oneOf: [
          {
            type: 'object',
            properties: {
              type: { type: 'string', enum: ['s3'] },
              access_key: { type: 'string' },
              secret_key: { type: 'string' },
              region: { type: 'string' },
              endpoint: { type: 'string' },
              force_path_style: { type: 'boolean' },
            },
            required: ['access_key', 'secret_key', 'region', 'endpoint'],
          },
        ],
      },
    },
    required: ['name', 'mount_point', 'credentials'],
  },
} as const

const deleteDiskSchema = {
  description: 'Delete External Disk',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
      diskId: { type: 'string' },
    },
    required: ['tenantId', 'diskId'],
  },
} as const

const linkDiskSchema = {
  description: 'Link External Disk',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
    },
    required: ['tenantId'],
  },
  body: {
    type: 'object',
    properties: {
      bucket_id: { type: 'string' },
    },
    required: ['bucket_id'],
  },
} as const

const listDisksSchema = {
  description: 'List Disks Credentials',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
    },
    required: ['tenantId'],
  },
} as const

interface CreateDiskRequest extends RequestGenericInterface {
  Body: FromSchema<typeof createDiskSchema.body>
  Params: {
    tenantId: string
  }
}

interface DeleteDiskRequest extends RequestGenericInterface {
  Params: {
    tenantId: string
    diskId: string
  }
}

interface ListDisksRequest extends RequestGenericInterface {
  Params: {
    tenantId: string
  }
}

interface LinkDisksRequest extends RequestGenericInterface {
  Body: FromSchema<typeof linkDiskSchema.body>
  Params: {
    tenantId: string
    diskId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)
  fastify.register(dbSuperUser)
  fastify.register(storage)

  fastify.post<CreateDiskRequest>(
    '/:tenantId/disks',
    {
      schema: createDiskSchema,
    },
    async (req, reply) => {
      const db = req.storage.db

      const provider = await db.createDisk({
        name: req.body.name,
        mount_point: req.body.mount_point,
        credentials: req.body.credentials,
      })

      reply.send({ provider })
    }
  )

  fastify.post<LinkDisksRequest>(
    '/:tenantId/disks/:diskId/mount',
    {
      schema: linkDiskSchema,
    },
    async (req, reply) => {
      const db = req.storage.db

      const bucketId = req.body.bucket_id

      const bucket = await db.withTransaction(async (tnx) => {
        const bucket = await tnx.findBucketById(bucketId, 'id', {
          forShare: true,
        })

        const objCount = await tnx.countObjectsInBucket(bucketId)

        if (objCount > 0) {
          throw new Error('Cannot mount disk to a non-empty bucket')
        }
        await tnx.updateBucket(bucketId, {
          disk_id: req.params.diskId,
        })

        return bucket
      })

      bucket.disk_id = req.params.diskId

      reply.send({ bucket })
    }
  )

  fastify.get<ListDisksRequest>(
    '/:tenantId/disks',
    { schema: listDisksSchema },
    async (req, reply) => {
      const credentials: string[] = []

      return reply.send(credentials)
    }
  )

  fastify.delete<DeleteDiskRequest>(
    '/:tenantId/disks/:diskId',
    { schema: deleteDiskSchema },
    async (req, reply) => {
      return reply.code(204).send()
    }
  )
}
