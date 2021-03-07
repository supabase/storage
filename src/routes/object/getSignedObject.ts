import { FastifyInstance } from 'fastify'
import { verifyJWT } from '../../utils/'
import { getObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { getSignedObjectRequest, signedToken } from '../../types/types'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

export default async function routes(fastify: FastifyInstance) {
  fastify.get<getSignedObjectRequest>('/signed/:bucketName/*', async (request, response) => {
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
}
