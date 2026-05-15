import { promises as fs } from 'fs'
import buildAdmin from '../admin-app'
import app from '../app'

type OpenApiOperation = {
  summary?: string
  requestBody?: unknown
  [key: string]: unknown
}

type OpenApiSpec = {
  paths?: Record<string, Record<string, unknown>>
}

const objectUploadSummaries = new Set([
  'Upload a new object',
  'Update the object at an existing key',
  'Uploads an object via a presigned URL',
])

const objectUploadRequestBody = {
  required: true,
  content: {
    'application/octet-stream': {
      schema: {
        type: 'string',
        format: 'binary',
      },
    },
    'multipart/form-data': {
      schema: {
        type: 'object',
        properties: {
          file: {
            type: 'string',
            format: 'binary',
          },
        },
        required: ['file'],
      },
    },
  },
}

function isOpenApiOperation(value: unknown): value is OpenApiOperation {
  return typeof value === 'object' && value !== null
}

function addObjectUploadRequestBodies(spec: OpenApiSpec) {
  for (const pathItem of Object.values(spec.paths ?? {})) {
    for (const method of ['post', 'put']) {
      const operation = pathItem[method]
      if (!isOpenApiOperation(operation) || typeof operation.summary !== 'string') {
        continue
      }

      if (objectUploadSummaries.has(operation.summary)) {
        operation.requestBody = objectUploadRequestBody
      }
    }
  }
}

;(async () => {
  // Export main API spec
  const storageApp = app({
    exposeDocs: true,
  })

  const response = await storageApp.inject({
    method: 'GET',
    url: '/documentation/json',
  })
  if (response.statusCode !== 200) {
    throw new Error('Unable to get api spec: ' + response.statusCode + ' ' + response.statusMessage)
  }

  const storageSpec = JSON.parse(response.body) as OpenApiSpec
  addObjectUploadRequestBodies(storageSpec)

  await fs.writeFile('static/api.json', JSON.stringify(storageSpec, null, 2))

  await storageApp.close()

  // Export admin API spec
  const adminApp = buildAdmin({
    exposeDocs: true,
  })

  const adminResponse = await adminApp.inject({
    method: 'GET',
    url: '/documentation/json',
  })
  if (adminResponse.statusCode !== 200) {
    throw new Error(
      'Unable to get admin api spec: ' +
        adminResponse.statusCode +
        ' ' +
        adminResponse.statusMessage
    )
  }

  await fs.writeFile('static/api-admin.json', adminResponse.body)

  await adminApp.close()
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
