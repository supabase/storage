import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getJwtSecret, getOwner, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { S3Backend } from '../../backend/s3'
import { FileBackend } from '../../backend/file'
import { GenericStorageBackend } from '../../backend/generic'

const { region, globalS3Bucket, globalS3Endpoint, storageBackendType } = getConfig()
let storageBackend: GenericStorageBackend

if (storageBackendType === 'file') {
  storageBackend = new FileBackend()
} else {
  storageBackend = new S3Backend(region, globalS3Endpoint)
}

const copyRequestBodySchema = {
  type: 'object',
  properties: {
    sourceKey: { type: 'string', examples: ['folder/source.png'] },
    bucketId: { type: 'string', examples: ['avatars'] },
    destinationKey: { type: 'string', examples: ['folder/destination.png'] },
  },
  required: ['sourceKey', 'bucketId', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string', examples: ['folder/destination.png'] },
  },
  required: ['Key'],
}
interface copyRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof copyRequestBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Copies an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: copyRequestBodySchema,
    summary,
    tags: ['object'],
  })

  fastify.post<copyRequestInterface>(
    '/copy',
    {
      schema,
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { sourceKey, destinationKey, bucketId } = request.body
      request.log.info(
        'sourceKey is %s and bucketName is %s and destinationKey is %s',
        sourceKey,
        bucketId,
        destinationKey
      )

      if (!isValidKey(destinationKey)) {
        const responseValue = createResponse(
          'The destination key contains invalid characters',
          '400',
          'Invalid key'
        )

        request.log.error(responseValue)
        return response.status(400).send(responseValue)
      }

      const jwtSecret = await getJwtSecret(request.tenantId)
      let owner
      try {
        owner = await getOwner(jwt, jwtSecret)
      } catch (err: any) {
        request.log.error(err)
        return response.status(400).send(createResponse(err.message, '400', err.message))
      }
      const objectResponse = await request.superUserPostgrest
        .from<Obj>('objects')
        .select('bucket_id, metadata')
        .match({
          name: sourceKey,
          bucket_id: bucketId,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: origObject } = objectResponse
      request.log.info({ origObject }, 'origObject')

      const newObject = Object.assign({}, origObject, {
        name: destinationKey,
        owner,
      })
      request.log.info({ origObject }, 'newObject')
      const {
        data: results,
        error,
        status,
      } = await request.postgrest
        .from<Obj>('objects')
        .insert([newObject], {
          returning: 'minimal',
        })
        .single()

      if (error) {
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      const s3SourceKey = `${request.tenantId}/${bucketId}/${sourceKey}`
      const s3DestinationKey = `${request.tenantId}/${bucketId}/${destinationKey}`
      const copyResult = await storageBackend.copyObject(
        globalS3Bucket,
        s3SourceKey,
        s3DestinationKey
      )
      return response.status(copyResult.httpStatusCode ?? 200).send({
        Key: `${bucketId}/${destinationKey}`,
      })
    }
  )
}
