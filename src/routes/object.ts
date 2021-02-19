import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import dotenv from 'dotenv'

dotenv.config()

interface requestGeneric extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
}

type Bucket = {
  id: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
}

type Object = {
  id: string
  bucketId: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
  lastAccessedAt: string
  metadata: string
  buckets: Bucket
}

export default async function routes(fastify: FastifyInstance) {
  fastify.get<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    const authHeader = request.headers.authorization
    const anonKey = process.env.ANON_KEY

    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    console.log(jwt)

    // @todo in kps, can we just ping localhost?
    const url = 'https://bjhaohmqunupljrqypxz.supabase.co/rest/v1'
    const postgrest = new PostgrestClient(url)
    postgrest.headers = {
      apiKey: anonKey,
    }
    postgrest.auth(jwt)

    const { bucketName } = request.params
    const objectName = request.params['*']

    console.log(bucketName, objectName)

    const { data: results, error } = await postgrest
      .from<Object>('objects')
      .select('*, buckets(*)')
      .match({
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    console.log(error)
    console.log(results)
    if (!results?.buckets) {
      // @todo why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    // send the object from s3
    const projectName = 'bjhaohmqunupljrqypxz'
    const s3Key = `${projectName}/${bucketName}/${objectName}`
    console.log(s3Key)
    const client = new S3Client({ region: 'us-east-1' })
    const command = new GetObjectCommand({
      Bucket: 'supa-storage-testing',
      Key: s3Key,
    })
    const data = await client.send(command)
    console.log(data)

    return response.status(200).header('content-type', data.ContentType).send(data.Body)
  })
  fastify.post('/object/:bucketId/:objectId', async (request, reply) => {})
  fastify.delete('/object/:bucketId/:objectId', async (request, reply) => {})
}
