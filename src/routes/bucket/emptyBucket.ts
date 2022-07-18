import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Bucket, Obj } from '../../types/types'
import { transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
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
const emptyBucketParamsSchema = {
  type: 'object',
  properties: {
    bucketId: { type: 'string', examples: ['avatars'] },
  },
  required: ['bucketId'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    message: { type: 'string', examples: ['Successfully emptied'] },
  },
}
interface emptyBucketRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof emptyBucketParamsSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Empty a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    params: emptyBucketParamsSchema,
    summary,
    tags: ['bucket'],
  })
  fastify.post<emptyBucketRequestInterface>(
    '/:bucketId/empty',
    {
      schema,
    },
    async (request, response) => {
      const { bucketId } = request.params

      const bucketResponse = await request.postgrest
        .from<Bucket>('buckets')
        .select('name')
        .eq('id', bucketId)
        .single()

      if (bucketResponse.error) {
        const { status, error } = bucketResponse
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: bucket } = bucketResponse
      const bucketName = bucket.name

      while (true) {
        const {
          data: objects,
          error: objectError,
          status: objectStatus,
        } = await request.postgrest
          .from<Obj>('objects')
          .select('name, id')
          .eq('bucket_id', bucketId)
          .limit(Math.floor(urlLengthLimit / (36 + 3))) // UUID + %2C lengths

        if (objectError) {
          request.log.error({ error: objectError }, 'error object')
          return response.status(400).send(transformPostgrestError(objectError, objectStatus))
        }
        request.log.info({ results: objects }, 'results')

        if (!(objects && objects.length > 0)) {
          break
        }

        const {
          error: deleteError,
          data: deleteData,
          status: deleteStatus,
        } = await request.postgrest
          .from<Obj>('objects')
          .delete()
          .in(
            'id',
            objects.map(({ id }) => id)
          )

        if (deleteError) {
          request.log.error({ error: deleteError }, 'error bucket')
          return response.status(400).send(transformPostgrestError(deleteError, deleteStatus))
        }

        if (deleteData && deleteData.length > 0) {
          const params = deleteData.map(({ name }) => {
            return `${request.tenantId}/${bucketName}/${name}`
          })
          // delete files from s3 asynchronously
          storageBackend.deleteObjects(globalS3Bucket, params)
        }

        if (deleteData?.length !== objects.length) {
          const deletedNames = new Set(deleteData?.map(({ name }) => name))
          const remainingNames = objects
            .filter(({ name }) => !deletedNames.has(name))
            .map(({ name }) => name)
          return response
            .status(400)
            .send(
              createResponse(
                `Cannot delete: ${remainingNames.join(
                  ' ,'
                )}, you may have SELECT but not DELETE permissions`,
                '400',
                'Cannot delete'
              )
            )
        }
      }
      return response.status(200).send(createResponse('Successfully emptied'))
    }
  )
}
