import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { getOwner, getPostgrestClient, signJWT, verifyJWT } from '../utils'
import {
  initClient,
  getObject,
  uploadObject,
  deleteObject,
  deleteObjects,
  copyObject,
} from '../utils/s3'
import { getConfig } from '../utils/config'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()

const client = initClient(region, globalS3Endpoint)
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

interface copyRequest extends RequestGenericInterface {
  Body: {
    sourceKey: string
    bucketName: string
    destinationKey: string
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
    if (!authHeader) {
      return response.status(403).send('Go away')
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
      .header('Content-Type', data.ContentType)
      .header('Cache-Control', data.CacheControl)
      .header('ETag', data.ETag)
      .header('Last-Modified', data.LastModified)
      .send(data.Body)
  })

  fastify.post<signRequest>('/sign/:bucketName/*', async (request, response) => {
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
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

    return response.status(200).send({ signedURL })
  })

  fastify.get<getSignedObjectRequest>('/signedobject/:bucketName/*', async (request, response) => {
    const { token } = request.query
    if (!token) {
      return response.status(403).send('Go away')
    }
    try {
      const payload = await verifyJWT(token)
      const { url } = payload as signedToken
      const s3Key = `${projectRef}/${url}`
      console.log(s3Key)
      const data = await getObject(client, globalS3Bucket, s3Key)

      return response
        .status(data.$metadata.httpStatusCode ?? 200)
        .header('Content-Type', data.ContentType)
        .header('Cache-Control', data.CacheControl ?? 'no-cache')
        .header('ETag', data.ETag)
        .header('Last-Modified', data.LastModified)
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
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const data = await request.file()

    // Can't seem to get the typing to work properly
    // https://github.com/fastify/fastify-multipart/issues/162
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
              cacheControl,
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
  })

  fastify.put<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to update the row
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
    }
    const jwt = authHeader.substring('Bearer '.length)
    const data = await request.file()
    /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
    const cacheControl: string = `max-age=${data.fields.cacheControl.value}` ?? 'no-cache'

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

    if (objectResponse.error) {
      const { status, error } = objectResponse
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
  })

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

  // @todo I think we need select permission here also since the return key is used to check if delete happened successfully and to delete it from s3
  fastify.delete<requestGeneric>('/object/:bucketName/*', async (request, response) => {
    // check if the user is able to insert that row
    const authHeader = request.headers.authorization
    if (!authHeader) {
      return response.status(403).send('Go away')
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
    if (!authHeader) {
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
