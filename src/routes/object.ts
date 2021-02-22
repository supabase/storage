import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import dotenv from 'dotenv'
import jwt from 'jsonwebtoken'

dotenv.config()

const {
  REGION: region,
  PROJECT_REF: projectRef,
  BUCKET_NAME: globalS3Bucket,
  SUPABASE_DOMAIN: supabaseDomain,
  ANON_KEY: anonKey,
  JWT_SECRET: jwtSecret,
} = process.env

const client = new S3Client({ region })
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
  metadata?: string
  buckets?: Bucket
}

function getPostgrestClient(jwt: string) {
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

function verifyJWT(token: string) {
  return new Promise((resolve, reject) => {
    jwt.verify(token, jwtSecret, (err, decoded) => {
      if (err) return reject(err)
      resolve(decoded)
    })
  })
}

async function getOwner(token: string) {
  const decodedJWT = await verifyJWT(token)
  return decodedJWT.sub
}

export default async function routes(fastify: FastifyInstance) {
  fastify.get<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const { data: results, error } = await postgrest
      .from<Object>('objects')
      .select('*, buckets(*)')
      .match({
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    console.log(error)
    // console.log(results)
    if (!results?.buckets) {
      // @todo why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    // send the object from s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    console.log(s3Key)
    const command = new GetObjectCommand({
      Bucket: globalS3Bucket,
      Key: s3Key,
    })
    const data = await client.send(command)
    console.log('done s3')

    return response
      .status(data.$metadata.httpStatusCode ?? 200)
      .header('content-type', data.ContentType)
      .send(data.Body)
  })

  fastify.post<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // @todo should upsert work?
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    const owner = getOwner(jwt)
    const { data: results, error } = await postgrest
      .from<Object>('objects')
      .insert([
        {
          name: objectName,
          owner: 'xxx', // decode jwt to get the owner
          bucketId: 'xxx', // do i need to make an api call for this? create a view
        },
      ])
      .single()

    // if successfully inserted, upload to s3
    // @todo return metadata of the uploaded object
  })

  fastify.delete('/object/:bucketId/:objectId', async (request, reply) => {})
}
