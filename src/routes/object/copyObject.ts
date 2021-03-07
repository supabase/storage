import { FastifyInstance } from 'fastify'
import { getPostgrestClient } from '../../utils'
import { copyObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { copyRequest, Obj } from '../../types/types'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

export default async function routes(fastify: FastifyInstance) {
  fastify.post<copyRequest>('/copy', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { sourceKey, destinationKey, bucketName } = request.body
    console.log(sourceKey, bucketName)

    const postgrest = getPostgrestClient(jwt)
    const objectResponse = await postgrest
      .from<Obj>('objects')
      .select('*, buckets(*)')
      .match({
        name: sourceKey,
        'buckets.name': bucketName,
      })
      .single()

    if (objectResponse.error) {
      const { status, error } = objectResponse
      console.log(error)
      return response.status(status).send(error.message)
    }
    const { data: origObject } = objectResponse
    console.log('origObject', origObject)

    if (!origObject.buckets) {
      // @todo why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    const newObject = Object.assign({}, origObject, {
      name: destinationKey,
      id: undefined,
      lastAccessedAt: undefined,
      createdAt: undefined,
      updatedAt: undefined,
      buckets: undefined,
    })
    console.log(newObject)
    const { data: results, error, status } = await postgrest
      .from<Obj>('objects')
      .insert([newObject], {
        returning: 'minimal',
      })
      .single()

    console.log(results, error)
    if (error) {
      return response.status(status).send(error.message)
    }

    const s3SourceKey = `${projectRef}/${bucketName}/${sourceKey}`
    const s3DestinationKey = `${projectRef}/${bucketName}/${destinationKey}`
    const copyResult = await copyObject(client, globalS3Bucket, s3SourceKey, s3DestinationKey)
    return response.status(copyResult.$metadata.httpStatusCode ?? 200).send({
      Key: destinationKey,
    })
  })
}
