import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { PostgrestClient } from '@supabase/postgrest-js'
import { DeleteObjectCommand, GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
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

const client = new S3Client({ region, runtime: 'node' })
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

    // @todo stream the response back instead of awaiting from s3 and sending back
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
    const owner = await getOwner(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
    const { data: bucket, error: bucketError } = await postgrest
      .from('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()
    if (bucketError) throw bucketError
    console.log(bucket)

    const { data: results, error } = await postgrest
      .from<Object>('objects')
      .insert([
        {
          name: objectName,
          owner: owner,
          bucketId: bucket.id,
        },
      ])
      .single()
    console.log(results, error)
    if (error) {
      return response.status(403).send('Go away')
    }

    // if successfully inserted, upload to s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    const data = await request.file()

    const paralellUploads3 = new Upload({
      client,
      params: {
        Bucket: globalS3Bucket,
        Key: s3Key,
        Body: data.file,
      },
    })

    await paralellUploads3.done()

    // @todo return metadata of the uploaded object
    return response.status(200).send('Uploaded')
  })

  // @todo add content-type metadata properly on upload and put
  // so that getobject sends the content-type header correctly
  fastify.put<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to update the row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    const owner = await getOwner(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
    const { data: bucket, error: bucketError } = await postgrest
      .from('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()
    if (bucketError) throw bucketError
    console.log(bucket)

    const { data: results, error } = await postgrest
      .from<Object>('objects')
      .update({
        lastAccessedAt: new Date().toISOString(),
      })
      .match({ bucketId: bucket.id, name: objectName })
      .limit(1)

    console.log('results: ', results)
    console.log('error: ', error)
    if (error || (results && results.length === 0)) {
      return response.status(403).send('Go away')
    }

    // if successfully inserted, upload to s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    const data = await request.file()

    const paralellUploads3 = new Upload({
      client,
      params: {
        Bucket: globalS3Bucket,
        Key: s3Key,
        Body: data.file,
      },
    })

    await paralellUploads3.done()

    // @todo return metadata of the uploaded object
    return response.status(200).send('Uploaded')
  })

  fastify.delete<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
    const { data: bucket, error: bucketError } = await postgrest
      .from('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()
    if (bucketError) throw bucketError
    console.log(bucket)

    const { data: results, error } = await postgrest.from<Object>('objects').delete().match({
      name: objectName,
      bucketId: bucket.id,
    })

    console.log(results, error)
    if (error || (results && results.length === 0)) {
      return response.status(403).send('Go away')
    }

    // if successfully deleted, delete from s3 too
    const s3Key = `${projectRef}/${bucketName}/${objectName}`

    const command = new DeleteObjectCommand({
      Bucket: globalS3Bucket,
      Key: s3Key,
    })
    const data = await client.send(command)
    console.log('done s3')

    return response.status(200).send('Deleted')
  })
}
