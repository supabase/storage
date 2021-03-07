import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner } from '../../utils'
import { uploadObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, Bucket } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const updateObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string' },
    '*': { type: 'string' },
  },
  required: ['bucketName', '*'],
} as const
interface updateObjectRequestInterface {
  Params: FromSchema<typeof updateObjectParamsSchema>
}

export default async function routes(fastify: FastifyInstance) {
  fastify.put<updateObjectRequestInterface>(
    '/:bucketName/*',
    { schema: { params: updateObjectParamsSchema } },
    async (request, response) => {
      // check if the user is able to update the row
      const authHeader = request.headers.authorization
      if (!authHeader) {
        return response.status(403).send('Go away')
      }
      const jwt = authHeader.substring('Bearer '.length)
      const data = await request.file()
      /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
      const cacheTime = data.fields.cacheControl?.value
      const cacheControl: string = `max-age=${cacheTime}` ?? 'no-cache'

      const { bucketName } = request.params
      const objectName = request.params['*']

      const postgrest = getPostgrestClient(jwt)
      const owner = await getOwner(jwt)
      // @todo how to merge these into one query?
      // i can create a view and add INSTEAD OF triggers..is that the way to do it?
      // @todo add unique constraint for just bucket names
      // @todo add types for all all postgrest select calls
      const bucketResponse = await postgrest
        .from<Bucket>('buckets')
        .select('id')
        .eq('name', bucketName)
        .single()

      if (bucketResponse.error) {
        const { error, status } = bucketResponse
        console.log(error)
        return response.status(status).send(error.message)
      }

      const { data: bucket } = bucketResponse
      console.log(bucket)

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .update({
          lastAccessedAt: new Date().toISOString(),
          owner,
          metadata: {
            mimetype: data.mimetype,
            cacheControl,
          },
        })
        .match({ bucketId: bucket.id, name: objectName })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(status).send(error.message)
      }

      // if successfully inserted, upload to s3
      const s3Key = `${projectRef}/${bucketName}/${objectName}`

      // @todo adding contentlength metadata will be harder since everything is streams
      const uploadResult = await uploadObject(
        client,
        globalS3Bucket,
        s3Key,
        data.file,
        data.mimetype,
        cacheControl
      )

      return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
        Key: s3Key,
      })
    }
  )
}
