import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
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

    // in kps, can we just ping localhost?
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
        // name: 'public/stripe.jpg',
        // 'buckets.id': '7078bc23-9dd6-460d-8b93-082254fee63a',
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    console.log(error)
    console.log(results)
    if (!results?.buckets) {
      // why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    // send the object from s3
    return response.status(200).send(results)
  })
  fastify.post('/object/:bucketId/:objectId', async (request, reply) => {})
  fastify.delete('/object/:bucketId/:objectId', async (request, reply) => {})
}
