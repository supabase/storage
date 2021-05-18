import {
  CopyObjectCommand,
  CopyObjectCommandOutput,
  DeleteObjectCommand,
  DeleteObjectCommandOutput,
  DeleteObjectsCommand,
  DeleteObjectsOutput,
  GetObjectCommand,
  GetObjectCommandOutput,
  HeadObjectCommand,
  HeadObjectOutput,
  ObjectIdentifier,
  S3Client,
  S3ClientConfig,
  ServiceOutputTypes,
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { NodeHttpHandler } from '@aws-sdk/node-http-handler'

export function initClient(region: string, endpoint?: string | undefined): S3Client {
  const params: S3ClientConfig = {
    region,
    runtime: 'node',
    requestHandler: new NodeHttpHandler({
      socketTimeout: 300000,
    }),
  }
  if (endpoint) {
    params.endpoint = endpoint
  }
  return new S3Client(params)
}

export async function getObject(
  client: S3Client,
  bucketName: string,
  key: string
): Promise<GetObjectCommandOutput> {
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: key,
  })
  const data = await client.send(command)
  return data
}

export async function uploadObject(
  client: S3Client,
  bucketName: string,
  key: string,
  body: NodeJS.ReadableStream,
  contentType: string,
  cacheControl: string
): Promise<ServiceOutputTypes> {
  const paralellUploadS3 = new Upload({
    client,
    params: {
      Bucket: bucketName,
      Key: key,
      /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
      Body: body,
      ContentType: contentType,
      CacheControl: cacheControl,
    },
  })

  return await paralellUploadS3.done()
}

export async function deleteObject(
  client: S3Client,
  bucket: string,
  key: string
): Promise<DeleteObjectCommandOutput> {
  const command = new DeleteObjectCommand({
    Bucket: bucket,
    Key: key,
  })
  return await client.send(command)
}

export async function copyObject(
  client: S3Client,
  bucket: string,
  source: string,
  destination: string
): Promise<CopyObjectCommandOutput> {
  const command = new CopyObjectCommand({
    Bucket: bucket,
    CopySource: `/${bucket}/${source}`,
    Key: destination,
  })
  return await client.send(command)
}

export async function deleteObjects(
  client: S3Client,
  bucket: string,
  prefixes: ObjectIdentifier[]
): Promise<DeleteObjectsOutput> {
  const command = new DeleteObjectsCommand({
    Bucket: bucket,
    Delete: {
      Objects: prefixes,
    },
  })
  return await client.send(command)
}

export async function headObject(
  client: S3Client,
  bucket: string,
  key: string
): Promise<HeadObjectOutput> {
  const command = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  })
  return await client.send(command)
}
