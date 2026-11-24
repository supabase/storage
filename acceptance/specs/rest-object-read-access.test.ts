import { describeAcceptance, encodePathSegments, getAcceptanceConfig } from '../support/config'
import { createRestClient, type HttpResponse } from '../support/http'
import {
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

// Every request is sent this many times: a Storage worker reads a bucket it has not seen
// before differently from one it has (it remembers which buckets exist and are public).
const REPEATS = 3

type Caller = 'none' | 'anon' | 'authenticated' | 'service'
type BucketState = 'public' | 'private' | 'missing'
type Outcome = 'found' | 'no-bucket' | 'no-object' | 'denied'

interface ReadRoute {
  method: 'GET' | 'HEAD'
  prefix: string
  // required: a valid JWT; optional: a missing or invalid JWT reads as anonymous; none:
  // the route reads no JWT at all.
  auth: 'required' | 'optional' | 'none'
  kind: 'asset' | 'info' | 'head'
}

const routes: ReadRoute[] = [
  { method: 'GET', prefix: '/object', auth: 'optional', kind: 'asset' },
  { method: 'GET', prefix: '/object/authenticated', auth: 'required', kind: 'asset' },
  { method: 'GET', prefix: '/object/info', auth: 'optional', kind: 'info' },
  { method: 'GET', prefix: '/object/info/authenticated', auth: 'required', kind: 'info' },
  { method: 'GET', prefix: '/object/info/public', auth: 'none', kind: 'info' },
  { method: 'HEAD', prefix: '/object', auth: 'optional', kind: 'head' },
  { method: 'HEAD', prefix: '/object/authenticated', auth: 'required', kind: 'head' },
  { method: 'HEAD', prefix: '/object/public', auth: 'none', kind: 'head' },
]

// What a read should give. A request without a JWT only reaches public buckets. The object
// of a public bucket is read as the super user, and so is any object for the service role;
// other roles read with RLS, and this assumes no policy lets them read a new bucket.
function expectedOutcome(
  route: ReadRoute,
  caller: Caller,
  bucket: BucketState,
  objectExists: boolean
): Outcome {
  if (route.auth === 'required' && caller === 'none') {
    return 'denied'
  }

  const authenticated = route.auth !== 'none' && caller !== 'none'
  if (bucket === 'missing' || (bucket === 'private' && !authenticated)) {
    return 'no-bucket'
  }

  const visible = bucket === 'public' || caller === 'service'
  return objectExists && visible ? 'found' : 'no-object'
}

interface ErrorResponse {
  statusCode?: string
  message?: string
}

function actualOutcome(
  route: ReadRoute,
  response: HttpResponse<ErrorResponse & { name?: string }>,
  objectKey: string,
  payload: string
): string {
  if (response.status === 200) {
    if (route.kind === 'asset' && response.body === payload) {
      return 'found'
    }
    if (route.kind === 'info' && response.json?.name === objectKey) {
      return 'found'
    }
    if (route.kind === 'head') {
      return 'found'
    }
  }

  // HEAD responses carry no body: only the status tells the outcomes apart.
  if (route.kind === 'head') {
    return `status ${response.status}`
  }

  if (response.status === 400 && response.json?.statusCode === '404') {
    if (response.json.message === 'Bucket not found') {
      return 'no-bucket'
    }
    if (response.json.message === 'Object not found') {
      return 'no-object'
    }
  }

  // Rejected before the route runs: the authorization header is required.
  if (response.status === 400 && response.json?.message?.includes("'authorization'")) {
    return 'denied'
  }

  return `status ${response.status}: ${response.body.slice(0, 200)}`
}

function expectedHeadOutcome(outcome: Outcome): string {
  return outcome === 'found' ? 'found' : 'status 400'
}

describeAcceptance(
  'REST object reads by caller and bucket',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    const config = getAcceptanceConfig()
    const client = createRestClient()
    const tokens: Partial<Record<Caller, string>> = {
      anon: config.anonKey,
      authenticated: config.authenticatedKey,
      service: config.serviceKey,
    }
    const callers = (['none', 'anon', 'authenticated', 'service'] as const).filter(
      (caller) => caller === 'none' || tokens[caller]
    )

    // Sends every read for `bucket` and returns a line per read whose outcome differs.
    async function checkReads(
      bucketName: string,
      bucket: BucketState,
      objects: Array<{ key: string; exists: boolean }>,
      payload: string
    ): Promise<string[]> {
      const mismatches: string[] = []

      for (const route of routes) {
        for (const caller of callers) {
          for (const { key, exists } of objects) {
            const outcome = expectedOutcome(route, caller, bucket, exists)
            const expected = route.kind === 'head' ? expectedHeadOutcome(outcome) : outcome
            const path = `${route.prefix}/${bucketName}/${encodePathSegments(key)}`

            for (let attempt = 1; attempt <= REPEATS; attempt++) {
              const response = await client.request<ErrorResponse & { name?: string }>(
                route.method,
                path,
                { token: tokens[caller] }
              )
              const actual = actualOutcome(route, response, key, payload)
              if (actual !== expected) {
                mismatches.push(
                  `${route.method} ${route.prefix} as ${caller}, ${bucket} bucket, ` +
                    `${exists ? 'existing' : 'missing'} object, attempt ${attempt}: ` +
                    `expected ${expected}, got ${actual}`
                )
              }
            }
          }
        }
      }

      return mismatches
    }

    async function setPublic(bucketName: string, isPublic: boolean) {
      await client.request('PUT', `/bucket/${bucketName}`, {
        body: { public: isPublic },
        expectedStatus: 200,
        token: requireServiceKey(config),
      })
    }

    it('serves GET, info and HEAD by caller for public, private and missing buckets', async () => {
      const publicBucket = uniqueBucketName('read-public')
      const privateBucket = uniqueBucketName('read-private')
      const missingBucket = uniqueBucketName('read-missing')
      const objectKey = uniqueObjectKey('read-access')
      const payload = `read-access-${config.runId}`
      const objects = [
        { key: objectKey, exists: true },
        { key: `${objectKey}.missing`, exists: false },
      ]

      try {
        await createRestBucket(publicBucket, { isPublic: true })
        await createRestBucket(privateBucket)
        await uploadRestObject(publicBucket, objectKey, payload)
        await uploadRestObject(privateBucket, objectKey, payload)

        const mismatches = [
          ...(await checkReads(publicBucket, 'public', objects, payload)),
          ...(await checkReads(privateBucket, 'private', objects, payload)),
          ...(await checkReads(missingBucket, 'missing', objects, payload)),
        ]

        expect(mismatches).toEqual([])
      } finally {
        await cleanupRestResources(publicBucket, [objectKey], client)
        await cleanupRestResources(privateBucket, [objectKey], client)
      }
    })

    it('follows a bucket that turns private, public again, empty and then goes away', async () => {
      const bucketName = uniqueBucketName('read-changes')
      const objectKey = uniqueObjectKey('read-changes')
      const payload = `read-changes-${config.runId}`
      const object = [{ key: objectKey, exists: true }]
      const removedObject = [{ key: objectKey, exists: false }]

      try {
        await createRestBucket(bucketName, { isPublic: true })
        await uploadRestObject(bucketName, objectKey, payload)
        const mismatches = await checkReads(bucketName, 'public', object, payload)

        await setPublic(bucketName, false)
        mismatches.push(...(await checkReads(bucketName, 'private', object, payload)))

        await setPublic(bucketName, true)
        mismatches.push(...(await checkReads(bucketName, 'public', object, payload)))

        await client.request('DELETE', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          expectedStatus: 200,
          token: requireServiceKey(config),
        })
        mismatches.push(...(await checkReads(bucketName, 'public', removedObject, payload)))

        await client.request('DELETE', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token: requireServiceKey(config),
        })
        mismatches.push(...(await checkReads(bucketName, 'missing', removedObject, payload)))

        expect(mismatches).toEqual([])
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })
  }
)
