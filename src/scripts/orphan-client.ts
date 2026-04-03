import { NdJsonTransform } from '@internal/streams/ndjson'
import axios from 'axios'
import path from 'path'
import { Transform, TransformCallback } from 'stream'
import { OrphanStreamEvent, writeStreamToJsonArray } from './orphan-client-stream'

const ADMIN_URL = process.env.ADMIN_URL
const ADMIN_API_KEY = process.env.ADMIN_API_KEY
const TENANT_ID = process.env.TENANT_ID
// bucket id to search, can handle multiple comma delimited buckets (aaa,bbb,ccc)
const BUCKET_ID = process.env.BUCKET_ID

// limits the number of delete operations to avoid overwhelming our queue
const DELETE_LIMIT = parseInt(process.env.DELETE_LIMIT || '1000000', 10)

const BEFORE = undefined // new Date().toISOString()

const FILE_PATH = (operation: string, bucketId: string) =>
  `../../dist/${operation}-${TENANT_ID}-${bucketId}-${Date.now()}-orphan-objects.json`

const client = axios.create({
  baseURL: ADMIN_URL,
  headers: {
    ApiKey: ADMIN_API_KEY,
  },
})

async function main() {
  const action = process.argv[2]

  if (!action) {
    console.error('Please provide an action: list or delete')
    return
  }

  if (!TENANT_ID) {
    console.error('Please provide a tenant ID')
    return
  }

  if (!BUCKET_ID) {
    console.error('Please provide a bucket ID')
    return
  }

  const buckets = BUCKET_ID.split(',')

  for (const bucket of buckets) {
    console.log(' ')
    console.log(`${action} items in bucket ${bucket}...`)
    if (action === 'list') {
      await listOrphans(TENANT_ID, bucket)
    } else {
      await deleteS3Orphans(TENANT_ID, bucket)
    }
  }
}

/**
 * List Orphan objects in a bucket
 * @param tenantId
 * @param bucketId
 */
async function listOrphans(tenantId: string, bucketId: string) {
  const request = await client.get(`/tenants/${tenantId}/buckets/${bucketId}/orphan-objects`, {
    responseType: 'stream',
    params: {
      before: BEFORE,
    },
  })

  const transformStream = new NdJsonTransform()
  request.data.on('error', (err: Error) => {
    transformStream.emit('error', err)
  })

  const jsonStream = request.data.pipe(transformStream)

  await writeStreamToJsonArray(jsonStream, path.resolve(__dirname, FILE_PATH('list', bucketId)))
}

/**
 * Deletes S3 orphan objects in a bucket
 * @param tenantId
 * @param bucketId
 */
async function deleteS3Orphans(tenantId: string, bucketId: string) {
  const request = await client.delete(`/tenants/${tenantId}/buckets/${bucketId}/orphan-objects`, {
    responseType: 'stream',
    data: {
      deleteS3Keys: true,
      before: BEFORE,
    },
  })

  const transformStream = new NdJsonTransform()
  request.data.on('error', (err: Error) => {
    transformStream.emit('error', err)
  })

  const jsonStream = request.data.pipe(transformStream)

  // Apply DELETE_LIMIT for delete operations
  let itemCount = 0
  const limitedStream = new Transform({
    objectMode: true,
    transform(chunk: OrphanStreamEvent, _encoding: string, callback: TransformCallback) {
      if (chunk.event === 'data' && chunk.value && Array.isArray(chunk.value)) {
        itemCount += chunk.value.length

        if (itemCount >= DELETE_LIMIT) {
          console.log(
            `Delete limit of ${DELETE_LIMIT} reached. Stopping after this batch. Ensure these operations complete before queuing additional jobs.`
          )
          this.push(chunk)
          callback()
          process.nextTick(() => {
            // Destroy the underlying HTTP request to stop further processing
            request.data.destroy()
            this.emit('error', new Error('DELETE_LIMIT_REACHED'))
          })
          return
        }
      }
      this.push(chunk)
      callback()
    },
  })

  await writeStreamToJsonArray(
    jsonStream.pipe(limitedStream),
    path.resolve(__dirname, FILE_PATH('delete', bucketId))
  )
}

main()
  .then(() => {
    console.log('Done')
  })
  .catch((e) => {
    process.exitCode = 1
    console.error('Error:', e)
  })
