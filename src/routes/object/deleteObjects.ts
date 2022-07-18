import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { objectSchema } from '../../schemas/object'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'
import { FileBackend } from '../../backend/file'
import { GenericStorageBackend } from '../../backend/generic'

const { region, globalS3Bucket, globalS3Endpoint, storageBackendType, urlLengthLimit } = getConfig()
let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(region, globalS3Endpoint)
}

const deleteObjectsParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketName'],
} as const
const deleteObjectsBodySchema = {
  type: 'object',
  properties: {
    prefixes: {
      type: 'array',
      items: { type: 'string' },
      minItems: 1,
      examples: [['folder/cat.png', 'folder/morecats.png']],
    },
  },
  required: ['prefixes'],
} as const
const successResponseSchema = {
  type: 'array',
  items: objectSchema,
}
interface deleteObjectsInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof deleteObjectsParamsSchema>
  Body: FromSchema<typeof deleteObjectsBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Delete multiple objects'

  const schema = createDefaultSchema(successResponseSchema, {
    body: deleteObjectsBodySchema,
    params: deleteObjectsParamsSchema,
    summary,
    tags: ['object'],
  })

  fastify.delete<deleteObjectsInterface>(
    '/:bucketName',
    {
      schema,
    },
    async (request, response) => {
      const { bucketName } = request.params
      const prefixes = request.body['prefixes']
      let results: { name: string }[] = []

      for (let i = 0; i < prefixes.length; ) {
        const prefixesSubset = []
        let urlParamLength = 0

        for (; i < prefixes.length && urlParamLength < urlLengthLimit; i++) {
          const prefix = prefixes[i]
          prefixesSubset.push(prefix)
          urlParamLength += encodeURIComponent(prefix).length + 9 // length of '%22%2C%22'
        }

        const objectResponse = await request.postgrest
          .from<Obj>('objects')
          .delete()
          .eq('bucket_id', bucketName)
          .in('name', prefixesSubset)

        if (objectResponse.error) {
          const { error, status } = objectResponse
          request.log.error({ error }, 'error object')
          return response.status(status).send(transformPostgrestError(error, status))
        }

        const { data } = objectResponse
        if (data.length > 0) {
          results = results.concat(data)

          // if successfully deleted, delete from s3 too
          const prefixesToDelete = data.map(
            ({ name }) => `${request.tenantId}/${bucketName}/${name}`
          )

          await storageBackend.deleteObjects(globalS3Bucket, prefixesToDelete)
        }
      }

      return response.status(200).send(results)
    }
  )
}
