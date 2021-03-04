import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { getPostgrestClient, getOwner } from '../utils'
import { deleteObjects, initClient } from '../utils/s3'
import { getConfig } from '../utils/config'
interface requestGeneric extends RequestGenericInterface {
  Params: {
    bucketId: string
    '*': string
  }
}

interface bucketCreateRequest extends RequestGenericInterface {
  Body: {
    name: string
  }
}

type Bucket = {
  id: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
}

type Obj = {
  id: string
  bucketId: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
  lastAccessedAt: string
  metadata?: Record<string, unknown>
  buckets?: Bucket
}

const { serviceKey, globalS3Bucket, projectRef, region, globalS3Endpoint } = getConfig()

const client = initClient(region, globalS3Endpoint)

export default async function routes(fastify: FastifyInstance) {
  // @todo I have enabled RLS only for objects table
  // makes writing the access policies a bit easier
  // tradeoff is that everyone can see what buckets there are
  // ahh looks like we do need RLS since users would be able to delete and empty buckets then
  // probably a RLS policy with just read permissions?
  fastify.get('/bucket', async (request, response) => {
    // get list of all buckets
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)
    const { data: results, error, status } = await postgrest.from<Bucket>('buckets').select('*')
    console.log(results, error)

    if (error) {
      return response.status(status).send(error.message)
    }

    response.send(results)
  })

  fastify.get<requestGeneric>('/bucket/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const postgrest = getPostgrestClient(jwt)
    const { data: results, error, status } = await postgrest
      .from<Bucket>('buckets')
      .select('*')
      .eq('id', bucketId)
      .single()

    console.log(results, error)

    if (error) {
      return response.status(status).send(error.message)
    }

    response.send(results)
  })

  fastify.post<requestGeneric>('/bucket/:bucketId/empty', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const postgrest = getPostgrestClient(jwt)

    const { data: bucket, error: bucketError, status: bucketStatus } = await postgrest
      .from<Bucket>('buckets')
      .select('name')
      .eq('id', bucketId)
      .single()

    console.log(bucket, bucketError)
    if (bucketError) {
      return response.status(bucketStatus).send(bucketError.message)
    }
    if (!bucket) {
      throw 'Should never happen'
    }
    const bucketName = bucket.name

    // @todo add pagination
    const { data: objects, error: objectError } = await postgrest
      .from<Obj>('objects')
      .select('name, id')
      .eq('bucketId', bucketId)
      .limit(1000)

    console.log(objects, objectError)
    if (objects) {
      const params = objects.map((ele) => {
        return {
          Key: `${projectRef}/${bucketName}/${ele.name}`,
        }
      })
      console.log(params)
      await deleteObjects(client, globalS3Bucket, params)

      const { error: deleteError } = await postgrest
        .from<Obj>('objects')
        .delete()
        .in(
          'id',
          objects.map((ele) => ele.id)
        )
      console.log(deleteError)
    }

    return response.status(200).send('Emptied')
  })

  fastify.delete<requestGeneric>('/bucket/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const userPostgrest = getPostgrestClient(jwt)
    const superUserPostgrest = getPostgrestClient(serviceKey)

    const { count: objectCount, error: objectError } = await superUserPostgrest
      .from<Obj>('objects')
      .select('id', { count: 'exact' })
      .eq('bucketId', bucketId)

    console.log(objectCount, objectError)
    if (objectCount && objectCount > 0) {
      return response.status(400).send('Bucket not empty')
    }

    const { data: results, error } = await userPostgrest
      .from<Bucket>('buckets')
      .delete()
      .eq('id', bucketId)
    console.log(results, error)
    return response.status(200).send('Deleted')
  })

  fastify.post<bucketCreateRequest>('/bucket', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const postgrest = getPostgrestClient(jwt)
    const owner = await getOwner(jwt)

    const { name: bucketName } = request.body

    const { data: results, error } = await postgrest.from<Bucket>('buckets').insert([
      {
        name: bucketName,
        owner,
      },
    ])
    console.log(results, error)
    return response.status(200).send(results)
  })
}
