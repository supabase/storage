import { describeAcceptance, encodePathSegments } from '../support/config'
import { AcceptanceHttpClient, createRestClient } from '../support/http'
import {
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
} from '../support/resources'

/**
 * Object versioning acceptance contract.
 *
 * The suite is black-box: there is no direct database access from `acceptance/`. Every
 * assertion about database state goes through two REST surfaces instead:
 *  - `GET /object/info/authenticated/:bucket/*` (optionally `?versionId=`) for a single
 *    row's state (`version`, `archived_at`, `is_delete_marker`, `is_versioned`).
 *  - `POST /object/list-v2/:bucket` with `noncurrentVersions`/`deleteMarkers` set to
 *    `include` to enumerate every row (current, archived, and delete markers) for a name.
 *
 * "Current" means `archived_at IS NULL`; at most one row per (bucket, name) can be current.
 */

interface BucketResponse {
  id: string
  name: string
  versioning_status?: string
}

interface ObjectVersionRow {
  name: string
  version: string
  archived_at: string | null
  is_delete_marker: boolean
  is_versioned: boolean
}

interface ListObjectsV2Response {
  folders: Array<{ name: string }>
  hasNext?: boolean
  nextCursor?: string
  objects: ObjectVersionRow[]
}

interface ObjectInfoResponse {
  id: string | null
  name: string
  version: string
  bucket_id: string
  size: number | null
  content_type: string | null
  cache_control: string | null
  etag: string | null
  metadata: Record<string, unknown> | null
  last_modified: string | null
  created_at: string | null
  archived_at: string | null
  is_delete_marker: boolean
  is_versioned: boolean
}

interface CopyResponse {
  Id?: string
  Key: string
  name?: string
  version?: string
  is_versioned?: boolean
  is_delete_marker?: boolean
  archived_at?: string | null
}

interface ErrorResponse {
  error?: string
  message?: string
  statusCode?: string
}

async function putObject(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  key: string,
  body: string,
  options: { contentType?: string; upsert?: boolean; expectedStatus?: number | number[] } = {}
) {
  return client.request<{ Id?: string; Key: string }>(
    'POST',
    `/object/${bucketName}/${encodePathSegments(key)}`,
    {
      body,
      expectedStatus: options.expectedStatus ?? 200,
      headers: {
        'content-type': options.contentType ?? 'text/plain',
        ...(options.upsert === false ? {} : { 'x-upsert': 'true' }),
      },
      token,
    }
  )
}

async function getInfo(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  key: string,
  options: { versionId?: string; expectedStatus?: number | number[] } = {}
) {
  const qs = options.versionId ? `?versionId=${encodeURIComponent(options.versionId)}` : ''
  return client.request<ObjectInfoResponse>(
    'GET',
    `/object/info/authenticated/${bucketName}/${encodePathSegments(key)}${qs}`,
    { expectedStatus: options.expectedStatus ?? 200, token }
  )
}

async function getContent(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  key: string,
  options: { versionId?: string; expectedStatus?: number | number[] } = {}
) {
  const qs = options.versionId ? `?versionId=${encodeURIComponent(options.versionId)}` : ''
  return client.request(
    'GET',
    `/object/authenticated/${bucketName}/${encodePathSegments(key)}${qs}`,
    { expectedStatus: options.expectedStatus ?? 200, token }
  )
}

async function deleteObject(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  key: string,
  options: { versionId?: string; expectedStatus?: number | number[] } = {}
) {
  const qs = options.versionId ? `?versionId=${encodeURIComponent(options.versionId)}` : ''
  return client.request('DELETE', `/object/${bucketName}/${encodePathSegments(key)}${qs}`, {
    expectedStatus: options.expectedStatus ?? 200,
    token,
  })
}

async function bulkDelete(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string,
  prefixes: Array<string | { path: string; versionId: string }>,
  expectedStatus: number | number[] = 200
) {
  return client.request<ObjectVersionRow[]>('DELETE', `/object/${bucketName}`, {
    body: { prefixes },
    expectedStatus,
    token,
  })
}

async function copyObject(
  client: AcceptanceHttpClient,
  token: string,
  params: {
    bucketId: string
    sourceKey: string
    sourceVersionId?: string
    destinationKey: string
    destinationBucket?: string
    upsert?: boolean
    expectedStatus?: number | number[]
  }
) {
  return client.request<CopyResponse>('POST', '/object/copy', {
    body: {
      bucketId: params.bucketId,
      destinationBucket: params.destinationBucket,
      destinationKey: params.destinationKey,
      sourceKey: params.sourceKey,
      sourceVersionId: params.sourceVersionId,
    },
    expectedStatus: params.expectedStatus ?? 200,
    headers: params.upsert ? { 'x-upsert': 'true' } : {},
    token,
  })
}

async function moveObject(
  client: AcceptanceHttpClient,
  token: string,
  params: {
    bucketId: string
    sourceKey: string
    sourceVersionId?: string
    destinationKey: string
    destinationBucket?: string
    expectedStatus?: number | number[]
  }
) {
  return client.request<{ message: string; Id?: string; Key?: string }>('POST', '/object/move', {
    body: {
      bucketId: params.bucketId,
      destinationBucket: params.destinationBucket,
      destinationKey: params.destinationKey,
      sourceKey: params.sourceKey,
      sourceVersionId: params.sourceVersionId,
    },
    expectedStatus: params.expectedStatus ?? 200,
    token,
  })
}

async function listAllVersions(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string
): Promise<ObjectVersionRow[]> {
  const rows: ObjectVersionRow[] = []
  let cursor: string | undefined

  do {
    const page = await client.request<ListObjectsV2Response>(
      'POST',
      `/object/list-v2/${bucketName}`,
      {
        body: {
          cursor,
          deleteMarkers: 'include',
          limit: 1000,
          noncurrentVersions: 'include',
          prefix: '',
          with_delimiter: false,
        },
        expectedStatus: 200,
        token,
      }
    )
    rows.push(...(page.json?.objects ?? []))
    cursor = page.json?.hasNext ? page.json?.nextCursor : undefined
  } while (cursor)

  return rows
}

function versionsOf(rows: ObjectVersionRow[], name: string): ObjectVersionRow[] {
  return rows.filter((row) => row.name === name)
}

function currentRowOf(rows: ObjectVersionRow[], name: string): ObjectVersionRow | undefined {
  return versionsOf(rows, name).find((row) => row.archived_at === null)
}

/**
 * A plain per-key `DELETE` on a versioned bucket only ever writes a fresh delete-marker
 * row (or, on a DISABLED bucket, no-ops if already gone) - it never actually frees the
 * history. `countObjectsInBucket` (used by bucket deletion's emptiness check) counts
 * every row regardless of `archived_at`/`is_delete_marker`, so cleanup must hard-delete
 * every row of every key via `{path, versionId}` before the bucket itself can be removed.
 */
async function purgeVersionedBucket(
  client: AcceptanceHttpClient,
  token: string,
  bucketName: string
) {
  const rows = await listAllVersions(client, token, bucketName).catch(() => [])
  const prefixes = rows.map((row) => ({ path: row.name, versionId: row.version }))

  if (prefixes.length > 0) {
    await bulkDelete(client, token, bucketName, prefixes).catch(() => undefined)
  }

  await client
    .request('DELETE', `/bucket/${bucketName}`, { expectedStatus: [200, 400, 404], token })
    .catch(() => undefined)
}

describeAcceptance(
  'Object versioning: bucket status lifecycle',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('defaults a new bucket to versioning DISABLED', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verdefault')

      try {
        await createRestBucket(bucketName)

        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('DISABLED')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('creates a bucket with versioning ENABLED at creation time', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vercreate')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('ENABLED')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('rejects transitioning a never-enabled bucket straight to SUSPENDED', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('versuspend')

      try {
        await createRestBucket(bucketName)

        const denied = await client.request<ErrorResponse>('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'SUSPENDED' },
          expectedStatus: 400,
          token,
        })
        expect(denied.json?.error).toBe('InvalidParameter')
        expect(denied.json?.message).toContain('Cannot transition bucket versioning status')

        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('DISABLED')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('walks the ENABLED -> SUSPENDED -> ENABLED transition matrix', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vertransition')

      try {
        await createRestBucket(bucketName)

        await client.request('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'ENABLED' },
          expectedStatus: 200,
          token,
        })
        let bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('ENABLED')

        await client.request('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'SUSPENDED' },
          expectedStatus: 200,
          token,
        })
        bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('SUSPENDED')

        await client.request('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'ENABLED' },
          expectedStatus: 200,
          token,
        })
        bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        expect(bucket.json?.versioning_status).toBe('ENABLED')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)

describeAcceptance(
  'Object versioning: uploads and version history',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('keeps every uploaded version independently readable, with its own metadata', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verupload')
      const key = uniqueObjectKey('history')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        await putObject(client, token, bucketName, key, 'content-1', {
          contentType: 'text/plain',
        })
        const v1 = (await getInfo(client, token, bucketName, key)).json
        expect(v1?.is_versioned).toBe(true)
        expect(v1?.archived_at).toBeNull()

        await putObject(client, token, bucketName, key, '{"n":2}', {
          contentType: 'application/json',
        })
        const v2 = (await getInfo(client, token, bucketName, key)).json
        expect(v2?.version).not.toBe(v1?.version)

        await putObject(client, token, bucketName, key, 'content-3', {
          contentType: 'text/plain',
        })
        const v3 = (await getInfo(client, token, bucketName, key)).json
        expect(v3?.version).not.toBe(v2?.version)

        // The current row is the latest upload; the others are preserved as history.
        expect(v3?.archived_at).toBeNull()
        expect(v3?.is_delete_marker).toBe(false)

        const excluded = await client.request<ListObjectsV2Response>(
          'POST',
          `/object/list-v2/${bucketName}`,
          {
            body: { limit: 100, prefix: '', with_delimiter: false },
            expectedStatus: 200,
            token,
          }
        )
        expect(excluded.json?.objects.map((o) => o.name)).toEqual([key])

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(3)
        expect(rows.filter((r) => r.archived_at === null)).toHaveLength(1)
        expect(new Set(rows.map((r) => r.version)).size).toBe(3)
        expect(rows.every((r) => r.is_versioned && !r.is_delete_marker)).toBe(true)

        // Each historical version is still independently downloadable, byte-for-byte
        // and metadata-for-metadata, by its own versionId.
        const first = await getContent(client, token, bucketName, key, {
          versionId: v1?.version,
        })
        expect(first.body).toBe('content-1')
        const firstInfo = await getInfo(client, token, bucketName, key, {
          versionId: v1?.version,
        })
        expect(firstInfo.json?.content_type).toBe('text/plain')
        expect(firstInfo.json?.archived_at).not.toBeNull()

        const second = await getContent(client, token, bucketName, key, {
          versionId: v2?.version,
        })
        expect(second.body).toBe('{"n":2}')
        const secondInfo = await getInfo(client, token, bucketName, key, {
          versionId: v2?.version,
        })
        expect(secondInfo.json?.content_type).toBe('application/json')

        const current = await getContent(client, token, bucketName, key)
        expect(current.body).toBe('content-3')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('rejects a non-upsert upload over a live object, but allows one over a delete marker', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verconflict')
      const key = uniqueObjectKey('conflict')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'original')

        const denied = await putObject(client, token, bucketName, key, 'blocked', {
          upsert: false,
          expectedStatus: 400,
        })
        expect((denied.json as ErrorResponse)?.error).toBe('Duplicate')
        expect((denied.json as ErrorResponse)?.statusCode).toBe('409')
        expect((await getContent(client, token, bucketName, key)).body).toBe('original')

        await deleteObject(client, token, bucketName, key)
        await getInfo(client, token, bucketName, key, { expectedStatus: 400 })

        // The current row is now a delete marker; a plain (non-upsert) upload is
        // treated as re-creating the key rather than colliding with it.
        await putObject(client, token, bucketName, key, 'revived', { upsert: false })
        expect((await getContent(client, token, bucketName, key)).body).toBe('revived')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('freezes ENABLED-era history under SUSPENDED, reusing a single mutable slot, and resumes archiving on re-enable', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('versuspendhist')
      const key = uniqueObjectKey('suspend-history')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        await putObject(client, token, bucketName, key, 'content-a')
        const vA = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'content-b')
        const vB = (await getInfo(client, token, bucketName, key)).json?.version

        await client.request('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'SUSPENDED' },
          expectedStatus: 200,
          token,
        })

        await putObject(client, token, bucketName, key, 'content-c')
        const vC = (await getInfo(client, token, bucketName, key)).json?.version
        expect((await getContent(client, token, bucketName, key)).body).toBe('content-c')

        let rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(3)

        // A second SUSPENDED-era write overwrites the same mutable slot in place:
        // no new row is added, and C's version disappears entirely (never archived).
        await putObject(client, token, bucketName, key, 'content-d')
        const vD = (await getInfo(client, token, bucketName, key)).json?.version
        expect((await getContent(client, token, bucketName, key)).body).toBe('content-d')
        expect(vD).not.toBe(vC)

        rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(3)
        await getContent(client, token, bucketName, key, {
          versionId: vC,
          expectedStatus: 400,
        })

        // Re-enabling versioning makes the next write archive that mutable slot too,
        // instead of overwriting it again.
        await client.request('PUT', `/bucket/${bucketName}`, {
          body: { versioning_status: 'ENABLED' },
          expectedStatus: 200,
          token,
        })
        await putObject(client, token, bucketName, key, 'content-e')

        rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(4)
        const current = currentRowOf(rows, key)
        expect(current?.archived_at).toBeNull()
        expect((await getContent(client, token, bucketName, key)).body).toBe('content-e')

        const dRow = rows.find((r) => r.version === vD)
        expect(dRow?.archived_at).not.toBeNull()
        expect((await getContent(client, token, bucketName, key, { versionId: vD })).body).toBe(
          'content-d'
        )
        expect(vA).toBeTruthy()
        expect(vB).toBeTruthy()
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)

describeAcceptance(
  'Object versioning: delete markers and undelete',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('hides the object behind a delete marker on delete, and restores the previous version when the marker is hard-deleted', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verundelete')
      const key = uniqueObjectKey('undelete')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'original')
        const contentVersion = (await getInfo(client, token, bucketName, key)).json?.version

        await deleteObject(client, token, bucketName, key)

        const hiddenInfo = await getInfo(client, token, bucketName, key, { expectedStatus: 400 })
        expect(hiddenInfo.json).toMatchObject({ error: 'not_found', statusCode: '404' })
        await getContent(client, token, bucketName, key, { expectedStatus: 400 })
        await client.request('HEAD', `/object/${bucketName}/${encodePathSegments(key)}`, {
          expectedStatus: 400,
          token,
        })

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(2)
        const marker = rows.find((r) => r.is_delete_marker)
        const content = rows.find((r) => !r.is_delete_marker)
        expect(marker?.archived_at).toBeNull()
        expect(content?.archived_at).not.toBeNull()
        expect(content?.version).toBe(contentVersion)

        // The marker itself is never directly fetchable, even by its own versionId -
        // only real content versions are.
        await getInfo(client, token, bucketName, key, {
          versionId: marker?.version,
          expectedStatus: 400,
        })
        expect(
          (await getContent(client, token, bucketName, key, { versionId: contentVersion })).body
        ).toBe('original')

        // Hard-deleting the current marker "undeletes" the key: the previous
        // content version is promoted back to current.
        await deleteObject(client, token, bucketName, key, { versionId: marker?.version })

        const restored = await getInfo(client, token, bucketName, key)
        expect(restored.json?.version).toBe(contentVersion)
        expect(restored.json?.archived_at).toBeNull()
        expect(restored.json?.is_delete_marker).toBe(false)
        expect((await getContent(client, token, bucketName, key)).body).toBe('original')

        const finalRows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(finalRows).toHaveLength(1)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('writes a delete marker for a key that was never uploaded (S3 parity)', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verghost')
      const key = uniqueObjectKey('never-uploaded')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        await deleteObject(client, token, bucketName, key)

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(1)
        expect(rows[0].is_delete_marker).toBe(true)
        expect(rows[0].archived_at).toBeNull()
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('hard-deletes with no marker on a DISABLED bucket', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verdisableddel')
      const key = uniqueObjectKey('disabled-delete')

      try {
        await createRestBucket(bucketName)
        await putObject(client, token, bucketName, key, 'gone-soon')

        await deleteObject(client, token, bucketName, key)

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(0)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('hard-deletes a specific noncurrent version without disturbing the current object', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verhardversion')
      const key = uniqueObjectKey('hard-version')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'content-1')
        const v1 = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'content-2')
        const v2 = (await getInfo(client, token, bucketName, key)).json?.version

        await deleteObject(client, token, bucketName, key, { versionId: v1 })

        expect((await getContent(client, token, bucketName, key)).body).toBe('content-2')
        await getContent(client, token, bucketName, key, { versionId: v1, expectedStatus: 400 })

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(1)
        expect(rows[0].version).toBe(v2)
        expect(rows[0].archived_at).toBeNull()
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('keeps a bucket non-empty while archived versions or delete markers remain, even though the current listing is empty', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vernonempty')
      const key = uniqueObjectKey('nonempty')

      await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
      await putObject(client, token, bucketName, key, 'will-be-hidden')
      await deleteObject(client, token, bucketName, key)

      const currentListing = await client.request<ListObjectsV2Response>(
        'POST',
        `/object/list-v2/${bucketName}`,
        { body: { limit: 100, prefix: '', with_delimiter: false }, expectedStatus: 200, token }
      )
      expect(currentListing.json?.objects).toEqual([])

      const denied = await client.request<ErrorResponse>('DELETE', `/bucket/${bucketName}`, {
        expectedStatus: 400,
        token,
      })
      expect(denied.json?.error).toBe('ResourceNotEmpty')
      expect(denied.json?.statusCode).toBe('409')

      await purgeVersionedBucket(client, token, bucketName)

      const missing = await client.request<ErrorResponse>('GET', `/bucket/${bucketName}`, {
        expectedStatus: 400,
        token,
      })
      expect(missing.json?.statusCode).toBe('404')
    })
  }
)

describeAcceptance(
  'Object versioning: bulk delete',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('writes delete markers for plain-path entries instead of hard-deleting', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verbulkmarker')
      const k1 = uniqueObjectKey('bulk-1')
      const k2 = uniqueObjectKey('bulk-2')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, k1, 'c1')
        await putObject(client, token, bucketName, k2, 'c2')

        const result = await bulkDelete(client, token, bucketName, [k1, k2])
        expect(result.json).toHaveLength(2)
        expect(result.json?.every((row) => row.is_delete_marker)).toBe(true)

        for (const key of [k1, k2]) {
          await getInfo(client, token, bucketName, key, { expectedStatus: 400 })
          const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
          expect(rows).toHaveLength(2)
          expect(rows.some((r) => r.is_delete_marker && r.archived_at === null)).toBe(true)
          expect(rows.some((r) => !r.is_delete_marker && r.archived_at !== null)).toBe(true)
        }
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('hard-deletes exact versions given {path, versionId}, promoting the previous version if the current one is removed', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verbulkversion')
      const key = uniqueObjectKey('bulk-version')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'v1')
        const v1 = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'v2')
        const v2 = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'v3')
        const v3 = (await getInfo(client, token, bucketName, key)).json?.version

        const result = await bulkDelete(client, token, bucketName, [
          { path: key, versionId: v3 as string },
        ])
        expect(result.json).toHaveLength(1)
        expect(result.json?.[0].version).toBe(v3)

        const promoted = await getInfo(client, token, bucketName, key)
        expect(promoted.json?.version).toBe(v2)
        expect(promoted.json?.archived_at).toBeNull()
        expect((await getContent(client, token, bucketName, key)).body).toBe('v2')

        await getContent(client, token, bucketName, key, { versionId: v1, expectedStatus: 200 })
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('handles a mixed request of plain paths, {path, versionId}, and a never-uploaded name in one call', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verbulkmixed')
      const kA = uniqueObjectKey('mixed-a')
      const kB = uniqueObjectKey('mixed-b')
      const kC = uniqueObjectKey('mixed-c-missing')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, kA, 'a1')
        await putObject(client, token, bucketName, kB, 'b1')
        const vB1 = (await getInfo(client, token, bucketName, kB)).json?.version
        await putObject(client, token, bucketName, kB, 'b2')

        const result = await bulkDelete(client, token, bucketName, [
          kA,
          { path: kB, versionId: vB1 as string },
          kC,
        ])
        expect(result.json).toHaveLength(3)

        // kA: current row is now a delete marker.
        await getInfo(client, token, bucketName, kA, { expectedStatus: 400 })

        // kB: only its noncurrent version was hard-deleted; the current content survives.
        expect((await getContent(client, token, bucketName, kB)).body).toBe('b2')
        await getContent(client, token, bucketName, kB, { versionId: vB1, expectedStatus: 400 })

        // kC: never existed, but a delete marker was still authorized and written.
        const kcRows = versionsOf(await listAllVersions(client, token, bucketName), kC)
        expect(kcRows).toHaveLength(1)
        expect(kcRows[0].is_delete_marker).toBe(true)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)

describeAcceptance(
  'Object versioning: copy',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('copies the current source version to an independent destination version, leaving source history untouched', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vercopy')
      const source = uniqueObjectKey('copy-src')
      const dest = uniqueObjectKey('copy-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'content-1')
        await putObject(client, token, bucketName, source, 'content-2')
        const sourceCurrent = (await getInfo(client, token, bucketName, source)).json?.version

        const copied = await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
        })
        expect(copied.json?.version).not.toBe(sourceCurrent)
        expect((await getContent(client, token, bucketName, dest)).body).toBe('content-2')

        const sourceRows = versionsOf(await listAllVersions(client, token, bucketName), source)
        expect(sourceRows).toHaveLength(2)
        expect(currentRowOf(sourceRows, source)?.version).toBe(sourceCurrent)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('copies an explicit noncurrent sourceVersionId rather than the latest content', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vercopyversion')
      const source = uniqueObjectKey('copy-old-src')
      const dest = uniqueObjectKey('copy-old-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'content-1')
        const v1 = (await getInfo(client, token, bucketName, source)).json?.version
        await putObject(client, token, bucketName, source, 'content-2')

        await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
          sourceVersionId: v1,
        })
        expect((await getContent(client, token, bucketName, dest)).body).toBe('content-1')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('requires x-upsert to copy over an existing destination, and archives (not destroys) the prior destination version', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vercopyupsert')
      const source = uniqueObjectKey('copy-up-src')
      const dest = uniqueObjectKey('copy-up-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'from-source')
        await putObject(client, token, bucketName, dest, 'dest-original')
        const destOriginalVersion = (await getInfo(client, token, bucketName, dest)).json?.version

        const denied = await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          expectedStatus: 400,
          sourceKey: source,
        })
        expect((denied.json as ErrorResponse)?.error).toBe('Duplicate')
        expect((await getContent(client, token, bucketName, dest)).body).toBe('dest-original')

        await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
          upsert: true,
        })
        expect((await getContent(client, token, bucketName, dest)).body).toBe('from-source')
        expect(
          (await getContent(client, token, bucketName, dest, { versionId: destOriginalVersion }))
            .body
        ).toBe('dest-original')

        const destRows = versionsOf(await listAllVersions(client, token, bucketName), dest)
        expect(destRows).toHaveLength(2)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('bypasses the upsert conflict for a delete-marker destination, and requires an explicit versionId to copy past a delete-marker source', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vercopymarker')
      const source = uniqueObjectKey('copy-marker-src')
      const dest = uniqueObjectKey('copy-marker-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        // Destination current row is a delete marker: copying without x-upsert succeeds.
        await putObject(client, token, bucketName, source, 'revive-me')
        await putObject(client, token, bucketName, dest, 'dest-was-here')
        await deleteObject(client, token, bucketName, dest)

        await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
        })
        expect((await getContent(client, token, bucketName, dest)).body).toBe('revive-me')

        // Source current row is a delete marker: copying without a versionId 404s,
        // but an explicit versionId of the earlier real content still works.
        const beforeDeleteVersion = (await getInfo(client, token, bucketName, source)).json?.version
        await deleteObject(client, token, bucketName, source)

        await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: uniqueObjectKey('copy-marker-fail'),
          expectedStatus: 400,
          sourceKey: source,
        })

        const dest2 = uniqueObjectKey('copy-marker-dst2')
        await copyObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest2,
          sourceKey: source,
          sourceVersionId: beforeDeleteVersion,
        })
        expect((await getContent(client, token, bucketName, dest2)).body).toBe('revive-me')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)

describeAcceptance(
  'Object versioning: move',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('leaves a delete marker at the source name and preserves its history when moving the current version', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vermove')
      const source = uniqueObjectKey('move-src')
      const dest = uniqueObjectKey('move-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'moved-content')
        const sourceVersion = (await getInfo(client, token, bucketName, source)).json?.version

        await moveObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
        })

        expect((await getContent(client, token, bucketName, dest)).body).toBe('moved-content')
        const destInfo = await getInfo(client, token, bucketName, dest)
        expect(destInfo.json?.version).not.toBe(sourceVersion)

        await getInfo(client, token, bucketName, source, { expectedStatus: 400 })

        const sourceRows = versionsOf(await listAllVersions(client, token, bucketName), source)
        expect(sourceRows).toHaveLength(2)
        expect(sourceRows.some((r) => r.is_delete_marker && r.archived_at === null)).toBe(true)
        expect(sourceRows.some((r) => r.version === sourceVersion && r.archived_at !== null)).toBe(
          true
        )
        expect(
          (await getContent(client, token, bucketName, source, { versionId: sourceVersion })).body
        ).toBe('moved-content')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('rejects moving onto an existing non-marker destination, leaving both keys unchanged', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vermoveconflict')
      const source = uniqueObjectKey('move-conflict-src')
      const dest = uniqueObjectKey('move-conflict-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'source-content')
        await putObject(client, token, bucketName, dest, 'dest-content')

        const denied = await moveObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          expectedStatus: 400,
          sourceKey: source,
        })
        expect((denied.json as ErrorResponse)?.error).toBe('Duplicate')

        expect((await getContent(client, token, bucketName, source)).body).toBe('source-content')
        expect((await getContent(client, token, bucketName, dest)).body).toBe('dest-content')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('allows moving onto a destination whose current row is a delete marker', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vermovemarker')
      const source = uniqueObjectKey('move-marker-src')
      const dest = uniqueObjectKey('move-marker-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'incoming-content')
        await putObject(client, token, bucketName, dest, 'dest-original')
        const destOriginalVersion = (await getInfo(client, token, bucketName, dest)).json?.version
        await deleteObject(client, token, bucketName, dest)

        await moveObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
        })

        expect((await getContent(client, token, bucketName, dest)).body).toBe('incoming-content')
        expect(
          (await getContent(client, token, bucketName, dest, { versionId: destOriginalVersion }))
            .body
        ).toBe('dest-original')
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('promotes the previous version back to current when moving away the current version by explicit versionId', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('vermovepromote')
      const source = uniqueObjectKey('move-promote-src')
      const dest = uniqueObjectKey('move-promote-dst')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, source, 'old-content')
        const oldVersion = (await getInfo(client, token, bucketName, source)).json?.version
        await putObject(client, token, bucketName, source, 'new-content')
        const currentVersion = (await getInfo(client, token, bucketName, source)).json?.version

        await moveObject(client, token, {
          bucketId: bucketName,
          destinationKey: dest,
          sourceKey: source,
          sourceVersionId: currentVersion,
        })

        expect((await getContent(client, token, bucketName, dest)).body).toBe('new-content')

        // Unlike a version-less move, the source key is not hidden behind a marker:
        // its previous version is promoted back to current.
        const restored = await getInfo(client, token, bucketName, source)
        expect(restored.json?.version).toBe(oldVersion)
        expect(restored.json?.archived_at).toBeNull()
        expect((await getContent(client, token, bucketName, source)).body).toBe('old-content')

        const sourceRows = versionsOf(await listAllVersions(client, token, bucketName), source)
        expect(sourceRows).toHaveLength(1)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)

describeAcceptance(
  'Object versioning: concurrency',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['versioning'],
  },
  () => {
    it('produces exactly one distinct version per concurrent upsert to the same key, with no lost writes', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verraceupload')
      const key = uniqueObjectKey('race-upload')
      const concurrency = 6
      const payloads = Array.from({ length: concurrency }, (_, i) => `race-payload-${i}`)

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })

        await Promise.all(
          payloads.map((payload) => putObject(client, token, bucketName, key, payload))
        )

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(concurrency)
        expect(rows.filter((r) => r.archived_at === null)).toHaveLength(1)
        expect(new Set(rows.map((r) => r.version)).size).toBe(concurrency)

        const bodies = await Promise.all(
          rows.map((row) =>
            getContent(client, token, bucketName, key, { versionId: row.version }).then(
              (res) => res.body
            )
          )
        )
        expect(new Set(bodies)).toEqual(new Set(payloads))
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('serializes a racing delete and upload on the same key into two ordered writes with exactly one current row', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verracedelete')
      const key = uniqueObjectKey('race-delete')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'initial')

        // The per-key advisory lock serializes these into two sequential writes
        // rather than dropping one: whichever runs first archives the initial
        // row and becomes current, then the second archives that result in turn
        // and becomes current itself. The initial row plus both operations'
        // own rows always total 3, with exactly one current at the end -
        // either the delete marker or the racing upload's content, depending
        // on which write was ordered last.
        await Promise.all([
          deleteObject(client, token, bucketName, key),
          putObject(client, token, bucketName, key, 'racing-upload'),
        ])

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(3)
        const currentRows = rows.filter((r) => r.archived_at === null)
        expect(currentRows).toHaveLength(1)
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })

    it('hard-deletes two different noncurrent versions concurrently without deadlocking or disturbing the current version', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('verracehard')
      const key = uniqueObjectKey('race-hard')

      try {
        await createRestBucket(bucketName, { versioningStatus: 'ENABLED' })
        await putObject(client, token, bucketName, key, 'v1')
        const v1 = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'v2')
        const v2 = (await getInfo(client, token, bucketName, key)).json?.version
        await putObject(client, token, bucketName, key, 'v3')
        const v3 = (await getInfo(client, token, bucketName, key)).json?.version

        await Promise.all([
          deleteObject(client, token, bucketName, key, { versionId: v1 }),
          deleteObject(client, token, bucketName, key, { versionId: v2 }),
        ])

        const rows = versionsOf(await listAllVersions(client, token, bucketName), key)
        expect(rows).toHaveLength(1)
        expect(rows[0].version).toBe(v3)
        expect(rows[0].archived_at).toBeNull()
      } finally {
        await purgeVersionedBucket(client, token, bucketName)
      }
    })
  }
)
