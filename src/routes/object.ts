import { FastifyInstance, RequestGenericInterface } from 'fastify'
import dotenv from 'dotenv'

import { getOwner, getPostgrestClient, signJWT, verifyJWT } from '../utils'
import { initClient, getObject, uploadObject, deleteObject, deleteObjects } from '../utils/s3'

dotenv.config()

const {
  REGION: region,
  PROJECT_REF: projectRef,
  BUCKET_NAME: globalS3Bucket,
  ANON_KEY: anonKey,
} = process.env

if (!region) {
  throw new Error('config not valid')
}
const client = initClient(region)
interface requestGeneric extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
}

interface signRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
  Body: {
    expiresIn: number
  }
}

interface getSignedObjectRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
  }
  Querystring: {
    token: string
  }
}

interface deleteObjectsRequest extends RequestGenericInterface {
  Params: {
    bucketName: string
  }
  Body: {
    prefixes: string[]
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

type signedToken = {
  url: string
}

// @todo better error handling everywhere
// @todo add console.error
export default async function routes(fastify: FastifyInstance) {
  fastify.get<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const objectResponse = await postgrest
      .from<Obj>('objects')
      .select('*, buckets(*)')
      .match({
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    if (objectResponse.error) {
      const { status, error } = objectResponse
      console.log(error)
      return response.status(status).send(error.message)
    }
    const { data: results } = objectResponse
    console.log(results)

    if (!results.buckets) {
      // @todo why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    // send the object from s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    console.log(s3Key)
    const data = await getObject(client, globalS3Bucket, s3Key)

    return response
      .status(data.$metadata.httpStatusCode ?? 200)
      .header('content-type', data.ContentType)
      .send(data.Body)
  })

  fastify.post<signRequest>('/sign/:bucketName/*', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const postgrest = getPostgrestClient(jwt)

    const { bucketName } = request.params
    const objectName = request.params['*']
    const { expiresIn } = request.body

    const objectResponse = await postgrest
      .from<Obj>('objects')
      .select('*, buckets(*)')
      .match({
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    if (objectResponse.error) {
      const { status, error } = objectResponse
      console.log(error)
      return response.status(status).send(error.message)
    }
    const { data: results } = objectResponse
    console.log(results)

    if (!results.buckets) {
      // @todo why is this check necessary?
      // if corresponding bucket is not found, i want the object also to not be returned
      // is it cos of https://github.com/PostgREST/postgrest/issues/1075 ?
      return response.status(404).send('not found')
    }

    console.log(`going to sign ${request.url}`)
    const urlParts = request.url.split('/')
    const urlToSign = urlParts.splice(2).join('/')
    const token = await signJWT({ url: urlToSign }, expiresIn)

    // @todo parse the url properly
    const signedURL = `/signedobject/${urlToSign}?token=${token}`

    return response.status(200).send(signedURL)
  })

  fastify.get<getSignedObjectRequest>('/signedobject/:bucketName/*', async (request, response) => {
    const { token } = request.query
    if (!token) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    try {
      const payload = await verifyJWT(token)
      const { url } = payload as signedToken
      const s3Key = `${projectRef}/${url}`
      console.log(s3Key)
      const data = await getObject(client, globalS3Bucket, s3Key)

      return response
        .status(data.$metadata.httpStatusCode ?? 200)
        .header('content-type', data.ContentType)
        .send(data.Body)
    } catch (err) {
      console.log(err)
      return response.send(400).send('Invalid token')
    }
  })

  fastify.post<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // @todo should upsert work?
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const data = await request.file()

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    const owner = await getOwner(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
    const bucketResponse = await postgrest
      .from<Bucket>('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()

    if (bucketResponse.error) {
      const { status, error } = bucketResponse
      console.log(error)
      return response.status(status).send(error.message)
    }

    const { data: bucket } = bucketResponse

    const { data: results, error, status } = await postgrest
      .from<Obj>('objects')
      .insert(
        [
          {
            name: objectName,
            owner: owner,
            bucketId: bucket.id,
            metadata: {
              mimetype: data.mimetype,
            },
          },
        ],
        {
          returning: 'minimal',
        }
      )
      .single()

    console.log(results, error)
    if (error) {
      return response.status(status).send(error.message)
    }

    // if successfully inserted, upload to s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    const uploadResult = await uploadObject(client, globalS3Bucket, s3Key, data.file, data.mimetype)

    return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
      Key: s3Key,
    })
  })

  fastify.put<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to update the row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const data = await request.file()

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
        },
      })
      .match({ bucketId: bucket.id, name: objectName })

    if (objectResponse.error) {
      const { status, error } = objectResponse
      return response.status(status).send(error.message)
    }

    // if successfully inserted, upload to s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`

    // @todo adding contentlength metadata will be harder since everything is streams
    const uploadResult = await uploadObject(client, globalS3Bucket, s3Key, data.file, data.mimetype)

    return response.status(uploadResult.$metadata.httpStatusCode ?? 200).send({
      Key: s3Key,
    })
  })

  // @todo I think we need select permission here also since the return key is used to check if delete happened successfully and to delete it from s3
  fastify.delete<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
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
    console.log(bucketResponse.body)
    const { data: bucket } = bucketResponse

    const objectResponse = await postgrest.from<Obj>('objects').delete().match({
      name: objectName,
      bucketId: bucket.id,
    })

    if (objectResponse.error) {
      const { error, status } = objectResponse
      console.log(error)
      return response.status(status).send(error.message)
    }
    const { data: results } = objectResponse
    console.log(results)

    if (results.length === 0) {
      // no rows returned, user doesn't have access to delete rows
      return response.status(403).send('Forbidden')
    }

    // if successfully deleted, delete from s3 too
    const s3Key = `${projectRef}/${bucketName}/${objectName}`
    await deleteObject(client, globalS3Bucket, s3Key)

    return response.status(200).send('Deleted')
  })

  // @todo I think we need select permission here also since the return key is used to check if delete happened successfully and to delete it from s3
  fastify.delete<deleteObjectsRequest>('/:bucketName', async (request, response) => {
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
    }
    if (!globalS3Bucket) {
      // @todo remove
      throw new Error('no s3 bucket')
    }
    const jwt = authHeader.substring('Bearer '.length)

    const { bucketName } = request.params
    const prefixes = request.body['prefixes']
    if (!prefixes) {
      return response.status(400).send('prefixes is required')
    }

    const postgrest = getPostgrestClient(jwt)
    // @todo how to merge these into one query?
    // i can create a view and add INSTEAD OF triggers..is that the way to do it?
    // @todo add unique constraint for just bucket names
    const { data: bucket, error: bucketError, status: bucketStatus } = await postgrest
      .from('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()

    console.log(bucket, bucketError)
    if (bucketError) {
      return response.status(bucketStatus).send(bucketError.message)
    }

    const objectResponse = await postgrest
      .from<Obj>('objects')
      .delete()
      .eq('bucketId', bucket.id)
      .in('name', prefixes)

    if (objectResponse.error) {
      const { error, status } = objectResponse
      console.log(error)
      return response.status(status).send(error.message)
    }

    const { data: results } = objectResponse
    if (results.length > 0) {
      // if successfully deleted, delete from s3 too
      const prefixesToDelete = results.map((ele) => {
        return { Key: `${projectRef}/${bucketName}/${ele.name}` }
      })

      await deleteObjects(client, globalS3Bucket, prefixesToDelete)
    }

    return response.status(200).send(results)
  })
}
