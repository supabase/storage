import axios from 'axios'
import { NdJsonTransform } from '@internal/streams/ndjson'
import fs from 'fs'
import path from 'path'

const ADMIN_URL = process.env.ADMIN_URL
const ADMIN_API_KEY = process.env.ADMIN_API_KEY
const TENANT_ID = process.env.TENANT_ID
const BUCKET_ID = process.env.BUCKET_ID

const BEFORE = undefined // new Date().toISOString()

const FILE_PATH = (operation: string) =>
  `../../dist/${operation}-${TENANT_ID}-${Date.now()}-orphan-objects.json`

const client = axios.create({
  baseURL: ADMIN_URL,
  headers: {
    ApiKey: ADMIN_API_KEY,
  },
})

interface OrphanObject {
  event: 'data'
  type: 's3Orphans'
  value: {
    name: string
    version: string
    size: number
  }[]
}

interface PingObject {
  event: 'ping'
}

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

  if (action === 'list') {
    await listOrphans(TENANT_ID, BUCKET_ID)
    return
  }

  await deleteS3Orphans(TENANT_ID, BUCKET_ID)
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

  await writeStreamToJsonArray(jsonStream, FILE_PATH('list'))
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

  await writeStreamToJsonArray(jsonStream, FILE_PATH('delete'))
}

/**
 * Writes the output to a JSON array
 * @param stream
 * @param relativePath
 */
async function writeStreamToJsonArray(
  stream: NodeJS.ReadableStream,
  relativePath: string
): Promise<void> {
  const filePath = path.resolve(__dirname, relativePath)
  const localFile = fs.createWriteStream(filePath)

  // Start with an empty array
  localFile.write('[\n')
  let isFirstItem = true

  return new Promise((resolve, reject) => {
    let receivedAnyData = false

    stream.on('data', (data: OrphanObject | PingObject) => {
      if (data.event === 'ping') {
        console.log('Received ping event, ignoring')
        return
      }

      if (data.event === 'data' && data.value && Array.isArray(data.value)) {
        receivedAnyData = true
        console.log(`Processing ${data.value.length} objects`)

        for (const item of data.value) {
          if (!isFirstItem) {
            localFile.write(',\n')
          } else {
            isFirstItem = false
          }

          localFile.write(JSON.stringify(item, null, 2))
        }
      } else {
        console.warn(
          'Received data with invalid format:',
          JSON.stringify(data).substring(0, 100) + '...'
        )
      }
    })

    stream.on('error', (err) => {
      console.error('Stream error:', err)
      localFile.end('\n]', () => {
        reject(err)
      })
    })

    stream.on('end', () => {
      localFile.write('\n]')
      localFile.end(() => {
        resolve()
      })

      if (!receivedAnyData) {
        console.warn(`No data was received! File might be empty: ${filePath}`)
      } else {
        // Check if the file exists and has content
        console.log(`Finished writing data to ${filePath}. Data was received and saved.`)
      }
    })
  })
}

main()
  .then(() => {
    console.log('Done')
  })
  .catch((e) => {
    console.error('Error:', e)
  })
