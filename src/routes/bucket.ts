import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import jwt from 'jsonwebtoken'
import dotenv from 'dotenv'

dotenv.config()
interface requestGeneric extends RequestGenericInterface {
  Params: {
    bucketId: string
    '*': string
  }
}

// @todo define as an interface expecting sub instead
type jwtType =
  | {
      aud: string
      exp: number
      sub: string
      email: string
      app_metadata: object
      user_metadata: object
      role: string
    }
  | undefined

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

type Object = {
  id: string
  bucketId: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
  lastAccessedAt: string
  metadata?: object
  buckets?: Bucket
}

const {
  REGION: region,
  PROJECT_REF: projectRef,
  BUCKET_NAME: globalS3Bucket,
  SUPABASE_DOMAIN: supabaseDomain,
  ANON_KEY: anonKey,
  JWT_SECRET: jwtSecret,
} = process.env

function getPostgrestClient(jwt: string) {
  if (!anonKey) {
    throw new Error('anonKey not found')
  }
  // @todo in kps, can we just ping localhost?
  const url = `https://${projectRef}.${supabaseDomain}/rest/v1`
  const postgrest = new PostgrestClient(url, {
    headers: {
      apiKey: anonKey,
      Authorization: `Bearer ${jwt}`,
    },
  })
  return postgrest
}

function verifyJWT(token: string): Promise<object | undefined> {
  if (!jwtSecret) {
    throw new Error('no jwtsecret')
  }
  return new Promise((resolve, reject) => {
    jwt.verify(token, jwtSecret, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded)
    })
  })
}

async function getOwner(token: string) {
  const decodedJWT = await verifyJWT(token)
  return (decodedJWT as jwtType)?.sub
}

export default async function routes(fastify: FastifyInstance) {
  // @todo I have enabled RLS only for objects table
  // makes writing the access policies a bit easier
  // tradeoff is that everyone can see what buckets there are
  // ahh looks like we do need RLS since users would be able to delete and empty buckets then
  // probably a RLS policy with just read permissions?
  fastify.get('/bucket', async (request, response) => {
    // get list of all buckets
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)
    const { data: results, error } = await postgrest.from<Bucket>('buckets').select('*')
    console.log(results, error)
    response.send(results)
  })

  fastify.get<requestGeneric>('/bucket/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    const postgrest = getPostgrestClient(jwt)
    const { data: results, error } = await postgrest
      .from<Bucket>('buckets')
      .select('*')
      .eq('id', bucketId)
      .single()
    console.log(results, error)
    response.send(results)
  })

  fastify.post<requestGeneric>('/bucket/:bucketId/empty', async (request, response) => {
    // @todo
  })

  fastify.delete<requestGeneric>('/bucket/:bucketId', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }

    const jwt = authHeader.substring('Bearer '.length)
    const { bucketId } = request.params
    // @todo we need to get a postgrest client with service token to check if bucket is empty
    // if not we may end up deleting a bucket where the user is not able to read any objects from the bucket
    const postgrest = getPostgrestClient(jwt)

    const { count: objectCount, error: objectError } = await postgrest
      .from<Object>('objects')
      .select('id', { count: 'exact' })
      .eq('bucketId', bucketId)

    console.log(objectCount, objectError)
    if (objectCount && objectCount > 0) {
      return response.status(400).send('Bucket not empty')
    }

    const { data: results, error } = await postgrest
      .from<Bucket>('buckets')
      .delete()
      .eq('id', bucketId)
    console.log(results, error)
    return response.status(200).send('Deleted')
  })

  fastify.post<bucketCreateRequest>('/bucket', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
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
