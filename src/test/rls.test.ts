import { randomUUID } from 'crypto'
import { Knex, knex } from 'knex'
import fs from 'fs'
import path from 'path'
import FormData from 'form-data'
import yaml from 'js-yaml'
import Mustache from 'mustache'
import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3'

import { StorageKnexDB } from '@storage/database'
import { createStorageBackend } from '@storage/backend'
import { getPostgresConnection } from '@internal/database'
import { getServiceKeyUser } from '@internal/database'
import { signJWT } from '@internal/auth'

import app from '../app'
import { getConfig } from '../config'
import { checkBucketExists } from './common'
import { Storage } from '../storage'

interface Policy {
  name: string
  permissions: string | string[]
  roles: string | string[]
  content: string
  tables: string | string[]
}

interface TestCase {
  description: string
  policies: string[]
  asserts: TestCaseAssert[]
  setup?: {
    create_bucket?: boolean
  }
}

interface TestCaseAssert {
  operation:
    | 'upload'
    | 'upload.upsert'
    | 'bucket.create'
    | 'bucket.get'
    | 'bucket.list'
    | 'bucket.delete'
    | 'bucket.update'
    | 'object.delete'
    | 'object.get'
    | 'object.list'
    | 'object.move'
    | 'object.copy'

  objectName?: string
  bucketName?: string
  useExistingBucketName?: string
  role?: string
  policies?: string[]
  status: number
  error?: string
}

interface RlsTestSpec {
  policies: Policy[]
  tests: TestCase[]
}

const testSpec = yaml.load(
  fs.readFileSync(path.resolve(__dirname, 'rls_tests.yaml'), 'utf8')
) as RlsTestSpec

const { serviceKey, tenantId, jwtSecret, databaseURL, storageS3Bucket, storageBackendType } =
  getConfig()
const backend = createStorageBackend(storageBackendType)
const client = backend.client

jest.setTimeout(10000)

describe('RLS policies', () => {
  let db: Knex

  beforeAll(async () => {
    // parse yaml file
    if (client instanceof S3Client) {
      const bucketExists = await checkBucketExists(client, storageS3Bucket)

      if (!bucketExists) {
        const createBucketCommand = new CreateBucketCommand({
          Bucket: storageS3Bucket,
        })
        await client.send(createBucketCommand)
      }
    }

    db = knex({
      connection: databaseURL,
      client: 'pg',
    })
  })

  let userId: string
  let jwt: string
  let storage: Storage
  beforeEach(async () => {
    userId = randomUUID()
    jwt = (await signJWT({ sub: userId, role: 'authenticated' }, jwtSecret, '1h')) as string

    await db.table('auth.users').insert({
      instance_id: '00000000-0000-0000-0000-000000000000',
      id: userId,
      aud: 'authenticated',
      role: 'authenticated',
      email: userId + '@supabase.io',
    })

    const adminUser = await getServiceKeyUser(tenantId)

    const pg = await getPostgresConnection({
      tenantId,
      user: adminUser,
      superUser: adminUser,
      host: 'localhost',
    })

    const knexDB = new StorageKnexDB(pg, {
      host: 'localhost',
      tenantId,
    })

    storage = new Storage(backend, knexDB)
  })

  afterEach(async () => {
    jest.clearAllMocks()
  })

  afterAll(async () => {
    await db.destroy()
    await (storage.db as StorageKnexDB).connection.dispose()
  })

  testSpec.tests.forEach((_test, index) => {
    it(_test.description, async () => {
      const content = fs.readFileSync(path.resolve(__dirname, 'rls_tests.yaml'), 'utf8')

      const runId = randomUUID()
      let bucketName: string = randomUUID()
      let objectName: string = randomUUID()
      const originalBucketName = bucketName

      const testScopedSpec = yaml.load(
        Mustache.render(content, {
          uid: userId,
          bucketName,
          objectName,
          runId,
        })
      ) as RlsTestSpec

      const test = testScopedSpec.tests[index]

      // Create requested policies
      const allPolicies = await Promise.all(
        test.policies.map(async (policyName) => {
          const policy = testScopedSpec.policies.find((policy) => policy.name === policyName)

          if (!policy) {
            throw new Error(`Policy ${policyName} not found`)
          }

          console.log(`Creating policy ${policyName}`)
          return await createPolicy(db, policy)
        })
      )

      // Prepare
      if (test.setup?.create_bucket !== false) {
        await storage.createBucket({
          name: bucketName,
          id: bucketName,
          public: false,
          owner: userId,
        })
        console.log(`Created bucket ${bucketName}`)
      }

      try {
        // Run Operations
        for (const assert of test.asserts) {
          if (assert.bucketName) {
            bucketName = assert.bucketName
            await storage.createBucket({
              name: bucketName,
              id: bucketName,
              public: false,
              owner: userId,
            })
            console.log(`Created bucket ${bucketName}`)
          }

          if (assert.useExistingBucketName) {
            bucketName = assert.useExistingBucketName
          }

          if (assert.objectName) {
            objectName = assert.objectName
          }

          let localPolicies: { name: string; table: string }[][] = []
          if (assert.policies && assert.policies.length > 0) {
            localPolicies = await Promise.all(
              assert.policies.map(async (policyName) => {
                const policy = testScopedSpec.policies.find((policy) => policy.name === policyName)

                if (!policy) {
                  throw new Error(`Policy ${policyName} not found`)
                }

                console.log(`Creating inline policy ${policyName}`)
                return await createPolicy(db, policy)
              })
            )
          }

          console.log(
            `Running operation ${assert.operation} with role ${assert.role || 'authenticated'}`
          )

          try {
            const response = await runOperation(assert.operation, {
              bucket: bucketName,
              objectName: objectName,
              jwt: assert.role === 'service' ? serviceKey : jwt,
            })

            console.log(
              `Operation ${assert.operation} with role ${assert.role || 'authenticated'} returned ${
                response.statusCode
              }`
            )

            bucketName = originalBucketName

            try {
              expect(response.statusCode).toBe(assert.status)
            } catch (e) {
              console.log(`Operation ${assert.operation} failed`, response.body)
              throw e
            }

            if (assert.error) {
              const body = await response.json()

              expect(body.message).toBe(assert.error)
            }
          } finally {
            console.log('deleting local policies')
            await Promise.all([
              ...localPolicies.map((policies) => {
                return Promise.all(
                  policies.map(async (policy) => {
                    console.log(
                      `RUNNING QUERY DROP POLICY IF EXISTS "${policy.name}" ON ${policy.table};`
                    )
                    await db.raw(`DROP POLICY "${policy.name}" ON ${policy.table};`)
                  })
                )
              }),
            ])
          }
        }
      } catch (e) {
        console.error('error', e)
        throw e
      } finally {
        await sleep(2000)
        const policiesToDelete = allPolicies.reduce((acc, policy) => {
          acc.push(...policy)
          return acc
        }, [] as { name: string; table: string }[])

        for (const policy of policiesToDelete) {
          await db.raw(`DROP POLICY IF EXISTS "${policy.name}" ON ${policy.table};`)
        }
      }
    })
  })
})

async function runOperation(
  operation: TestCaseAssert['operation'],
  options: { bucket: string; jwt: string; objectName: string }
) {
  const { jwt, bucket, objectName } = options

  switch (operation) {
    case 'upload':
      return uploadFile(bucket, objectName, jwt)
    case 'upload.upsert':
      return uploadFile(bucket, objectName, jwt, true)
    case 'bucket.list':
      return app().inject({
        method: 'GET',
        url: `/bucket`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
      })
    case 'bucket.get':
      return app().inject({
        method: 'GET',
        url: `/bucket/${bucket}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
      })
    case 'bucket.create':
      return app().inject({
        method: 'POST',
        url: `/bucket`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
        payload: {
          name: bucket,
        },
      })
    case 'bucket.update':
      console.log(`updating bucket ${bucket}`)
      return app().inject({
        method: 'PUT',
        url: `/bucket/${bucket}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
        payload: {
          public: true,
        },
      })
    case 'bucket.delete':
      return app().inject({
        method: 'DELETE',
        url: `/bucket/${bucket}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
      })
    case 'object.delete':
      return app().inject({
        method: 'DELETE',
        url: `/object/${bucket}/${objectName}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
      })
    case 'object.get':
      return app().inject({
        method: 'GET',
        url: `/object/authenticated/${bucket}/${objectName}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
      })
    case 'object.list':
      return app().inject({
        method: 'POST',
        url: `/object/list/${bucket}`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
        payload: {
          prefix: '',
          sortBy: {
            column: 'name',
            order: 'asc',
          },
        },
      })
    case 'object.move':
      return app().inject({
        method: 'POST',
        url: `/object/move`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
        payload: {
          bucketId: bucket,
          sourceKey: objectName,
          destinationKey: 'moved_' + objectName,
        },
      })
    case 'object.copy':
      return app().inject({
        method: 'POST',
        url: `/object/copy`,
        headers: {
          authorization: `Bearer ${jwt}`,
        },
        payload: {
          bucketId: bucket,
          sourceKey: objectName,
          destinationKey: 'copied_' + objectName,
        },
      })
    default:
      throw new Error(`Operation ${operation} not supported`)
  }
}

async function createPolicy(db: Knex, policy: Policy) {
  const { name, content } = policy
  let { tables, roles, permissions } = policy

  if (!Array.isArray(roles)) {
    roles = [roles]
  }

  if (!Array.isArray(tables)) {
    tables = [tables]
  }

  if (!Array.isArray(permissions)) {
    permissions = [permissions]
  }

  const created: Promise<{ table: string; name: string }>[] = []

  tables.forEach((table) => {
    ;(roles as string[]).forEach((role) => {
      ;(permissions as string[]).forEach((permission) => {
        console.log(
          'RUNNING QUERY ' +
            `CREATE POLICY "${name}_${permission}" ON ${table} FOR ${permission} TO "${role}" ${content}`
        )
        created.push(
          db
            .raw(
              `CREATE POLICY "${name}_${permission}" ON ${table} AS PERMISSIVE FOR ${permission} TO "${role}" ${content}`
            )
            .then(() => ({
              name: `${name}_${permission}`,
              table,
            }))
        )
      })
    })
  })

  return Promise.all(created)
}

async function uploadFile(bucket: string, fileName: string, jwt: string, upsert?: boolean) {
  const testFile = fs.createReadStream(path.resolve(__dirname, 'assets', 'sadcat.jpg'))
  const form = new FormData()
  form.append('file', testFile)
  const headers = Object.assign({}, form.getHeaders(), {
    authorization: `Bearer ${jwt}`,
    ...(upsert ? { 'x-upsert': 'true' } : {}),
  })

  return app().inject({
    method: 'POST',
    url: `/object/${bucket}/${fileName}`,
    headers,
    payload: form,
  })
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
