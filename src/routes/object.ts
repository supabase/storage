import { FastifyInstance, RequestGenericInterface } from 'fastify'
import {
  Bucket,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import dotenv from 'dotenv'

import { getOwner, getPostgrestClient } from '../utils'

dotenv.config()

const {
  REGION: region,
  PROJECT_REF: projectRef,
  BUCKET_NAME: globalS3Bucket,
  ANON_KEY: anonKey,
} = process.env

const client = new S3Client({ region, runtime: 'node' })
interface requestGeneric extends RequestGenericInterface {
  Params: {
    bucketName: string
    '*': string
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

// @todo better error handling everywhere
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

    const { data: results, error, status } = await postgrest
      .from<Obj>('objects')
      .select('*, buckets(*)')
      .match({
        name: objectName,
        'buckets.name': bucketName,
      })
      .single()

    console.log(error, results)
    if (error) {
      return response.status(status).send(error.message)
    }
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
    const data = await request.file()

    const { bucketName } = request.params
    const objectName = request.params['*']

    const postgrest = getPostgrestClient(jwt)
    const owner = await getOwner(jwt)
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

    const paralellUploadS3 = new Upload({
      client,
      params: {
        Bucket: globalS3Bucket,
        Key: s3Key,
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        Body: data.file,
        ContentType: data.mimetype,
      },
    })

    const uploadResult = await paralellUploadS3.done()

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
    const { data: bucket, error: bucketError, status: bucketStatus } = await postgrest
      .from('buckets')
      .select('id')
      .eq('name', bucketName)
      .single()

    console.log(bucket, bucketError)
    if (bucketError) {
      return response.status(bucketStatus).send(bucketError.message)
    }

    const { data: results, error, status, statusText } = await postgrest
      .from<Obj>('objects')
      .update({
        lastAccessedAt: new Date().toISOString(),
        owner,
        metadata: {
          mimetype: data.mimetype,
        },
      })
      .match({ bucketId: bucket.id, name: objectName })

    console.log(error, results)

    if (error) {
      return response.status(status).send(error.message)
    }
    if (results && results.length === 0) {
      return response.status(status).send(statusText)
    }

    // if successfully inserted, upload to s3
    const s3Key = `${projectRef}/${bucketName}/${objectName}`

    // @todo adding contentlength metadata will be harder since everything is streams
    const paralellUploadS3 = new Upload({
      client,
      params: {
        Bucket: globalS3Bucket,
        Key: s3Key,
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        Body: data.file,
        ContentType: data.mimetype,
      },
    })

    const uploadResult = await paralellUploadS3.done()

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

    const { data: results, error, status } = await postgrest.from<Obj>('objects').delete().match({
      name: objectName,
      bucketId: bucket.id,
    })

    console.log(results, error)
    if (error) {
      return response.status(status).send(error.message)
    }
    if (results && results.length === 0) {
      // no rows returned, user doesn't have access to delete rows
      return response.status(403).send('Forbidden')
    }

    // if successfully deleted, delete from s3 too
    const s3Key = `${projectRef}/${bucketName}/${objectName}`

    const command = new DeleteObjectCommand({
      Bucket: globalS3Bucket,
      Key: s3Key,
    })
    await client.send(command)
    console.log('done s3')

    return response.status(200).send('Deleted')
  })

  // @todo I think we need select permission here also since the return key is used to check if delete happened successfully and to delete it from s3
  fastify.delete<deleteObjectsRequest>('/:bucketName', async (request, response) => {
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader || !anonKey) {
      return response.status(403).send('Go away')
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

    const { data: results, error, status } = await postgrest
      .from<Obj>('objects')
      .delete()
      .eq('bucketId', bucket.id)
      .in('name', prefixes)

    console.log(results, error)
    if (error) {
      return response.status(status).send(error.message)
    }

    if (results && results.length > 0) {
      // if successfully deleted, delete from s3 too
      const prefixesToDelete = results.map((ele) => {
        return { Key: `${projectRef}/${bucketName}/${ele.name}` }
      })

      const command = new DeleteObjectsCommand({
        Bucket: globalS3Bucket,
        Delete: {
          Objects: prefixesToDelete,
        },
      })
      await client.send(command)
      console.log('done s3')
    }

    return response.status(200).send(results)
  })
}
