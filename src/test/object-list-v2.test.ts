import { randomUUID } from 'node:crypto'
import {
  type DatabaseTransaction,
  getPostgresConnection,
  getServiceKeyUser,
} from '@internal/database'
import { ListObjectsV2Result } from '@storage/object'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig } from '../config'
import { Obj } from '../storage'
import { useMockObject, useMockQueue } from './common'
import { withDeleteEnabled } from './utils/storage'

const { serviceKeyAsync, tenantId } = getConfig()
let appInstance: FastifyInstance
let serviceKey: string = ''

let tnx: DatabaseTransaction | undefined
async function getSuperuserPostgrestClient() {
  const superUser = await getServiceKeyUser(tenantId)

  const conn = await getPostgresConnection({
    superUser,
    user: superUser,
    tenantId,
    host: 'localhost',
  })
  tnx = await conn.transaction()

  return tnx
}

async function insertObjects(
  db: DatabaseTransaction,
  objects:
    | Array<Partial<Obj> & { bucket_id: string; name: string }>
    | (Partial<Obj> & { bucket_id: string; name: string })
) {
  const rows = Array.isArray(objects) ? objects : [objects]

  for (const row of rows) {
    const entries = Object.entries(row)
    await db.query({
      text: `
        INSERT INTO objects (${entries.map(([column]) => column).join(', ')})
        VALUES (${entries.map((_, index) => `$${index + 1}`).join(', ')})
      `,
      values: entries.map(([, value]) => value),
    })
  }
}

async function deleteObjectsByName(
  db: DatabaseTransaction,
  bucketId: string,
  names: string | string[]
) {
  await db.query({
    text: `
      DELETE FROM objects
      WHERE bucket_id = $1
        AND name = ANY($2::text[])
    `,
    values: [bucketId, Array.isArray(names) ? names : [names]],
  })
}

useMockObject()
useMockQueue()

beforeEach(() => {
  getConfig({ reload: true })
  appInstance = app()
})

afterEach(async () => {
  if (tnx) {
    await tnx.commit()
  }
  await appInstance.close()
})

const LIST_V2_BUCKET = 'list-v2-sorting-test-bucket'

// Helper to convert a number into a 3-letter string (aaa ... zzz with some uppercase)
function toName(n: number): string {
  const a = 97 // 'a'
  const first = String.fromCharCode(a + (Math.floor(n / (26 * 26)) % 26))
  const second = String.fromCharCode(a + (Math.floor(n / 26) % 26))
  const third = String.fromCharCode(a + (n % 26))
  const name = first + second + third
  if (n >= 1 && n <= 3) {
    return name.toUpperCase()
  }
  return name
}

function createUpload(name: string, content: string) {
  return new File([content], name)
}

function shuffleArray<T>(array: T[]): T[] {
  const shuffled = [...array]
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]]
  }
  return shuffled
}

const SORTED_OBJECTS: string[] = []
const SORTED_FOLDERS: string[] = []
const NESTED_OBJECTS: string[] = []
const PREFIX_OBJECTS: Record<string, { sorted: string[]; created: string[]; updated: string[] }> =
  {}
const TEST_PREFIX = 'aal'

// Generate sorted list of objects/folders
for (let i = 0; i < 30; i++) {
  const name = toName(i)
  if (i > 5) {
    SORTED_OBJECTS.push(name + '.txt')
  }
  if (i < 18) {
    const folder = name + '/'
    SORTED_FOLDERS.push(folder)

    const nestedCount = name === TEST_PREFIX ? 9 : 3
    for (let j = 0; j < nestedCount; j++) {
      const objectPath = `${folder}dummy-${name}-${j}.txt`
      NESTED_OBJECTS.push(objectPath)
      PREFIX_OBJECTS[folder] ??= { sorted: [], created: [], updated: [] }
      PREFIX_OBJECTS[folder].sorted.push(objectPath)
    }
  }
}

// Sort the arrays since uppercase letters may have changed the order
SORTED_OBJECTS.sort()
SORTED_FOLDERS.sort()
for (const folder of Object.keys(PREFIX_OBJECTS)) {
  PREFIX_OBJECTS[folder].sorted.sort()
}

// Combine all paths for creation
const ALL_PATHS = [...SORTED_OBJECTS, ...NESTED_OBJECTS].sort()

// Lists of objects and folders in sorted
const CREATION_ORDER_OBJECTS: string[] = []
const UPDATE_ORDER_OBJECTS: string[] = []
const CREATION_ORDER_FOLDERS: string[] = []
const CREATION_ORDER_ALL: string[] = []
const UPDATE_ORDER_ALL: string[] = []

beforeAll(async () => {
  serviceKey = await serviceKeyAsync
  appInstance = app()

  // Create bucket
  await appInstance.inject({
    method: 'POST',
    url: `/bucket`,
    headers: {
      authorization: `Bearer ${serviceKey}`,
    },
    payload: {
      name: LIST_V2_BUCKET,
    },
  })

  const shuffledPaths = shuffleArray(ALL_PATHS)

  // Create all objects in random order
  for (const path of shuffledPaths) {
    if (path.includes('/')) {
      // root folders in creation order
      const rootFolder = path.split('/')[0] + '/'
      if (!CREATION_ORDER_FOLDERS.includes(rootFolder)) {
        CREATION_ORDER_FOLDERS.push(rootFolder)
      }
      PREFIX_OBJECTS[rootFolder].created.push(path)
      PREFIX_OBJECTS[rootFolder].updated.push(path)
    } else {
      // root objects in creation order
      CREATION_ORDER_OBJECTS.push(path)
      UPDATE_ORDER_OBJECTS.push(path)
    }
    CREATION_ORDER_ALL.push(path)
    UPDATE_ORDER_ALL.push(path)
    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_BUCKET}/${path}`,
      payload: createUpload(path, 'test content'),
      headers: {
        authorization: serviceKey,
      },
    })
  }

  const headers = {
    authorization: serviceKey,
    'x-upsert': 'true',
  }

  // update a few objects to make updated_at different than created_at
  for (let i = 0; i < 10; i++) {
    const firstItem = UPDATE_ORDER_OBJECTS.shift()!
    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_BUCKET}/${firstItem}`,
      payload: createUpload(firstItem, 'test content'),
      headers,
    })
    UPDATE_ORDER_OBJECTS.push(firstItem)

    // re-arrange item in flat object list to updated order
    UPDATE_ORDER_ALL.splice(UPDATE_ORDER_ALL.indexOf(firstItem), 1)
    UPDATE_ORDER_ALL.push(firstItem)
  }

  // switch to Object.entries(PREFIX_OBJECTS) to test all prefixes
  const prefixRoot = TEST_PREFIX + '/'
  const obj = PREFIX_OBJECTS[prefixRoot]
  const firstPrefixItem = obj.updated.shift()!
  await appInstance.inject({
    method: 'POST',
    url: `/object/${LIST_V2_BUCKET}/${firstPrefixItem}`,
    payload: createUpload(firstPrefixItem, 'test content'),
    headers,
  })
  PREFIX_OBJECTS[prefixRoot].updated.push(firstPrefixItem)

  // re-arrange item in flat object list to updated order of nested item
  UPDATE_ORDER_ALL.splice(UPDATE_ORDER_ALL.indexOf(firstPrefixItem), 1)
  UPDATE_ORDER_ALL.push(firstPrefixItem)

  await appInstance.close()
}, 300000)

afterAll(async () => {
  appInstance = app()

  // Empty the bucket
  await appInstance.inject({
    method: 'POST',
    url: `/bucket/${LIST_V2_BUCKET}/empty`,
    headers: {
      authorization: `Bearer ${serviceKey}`,
    },
  })

  // Delete the bucket
  await appInstance.inject({
    method: 'DELETE',
    url: `/bucket/${LIST_V2_BUCKET}`,
    headers: {
      authorization: `Bearer ${serviceKey}`,
    },
  })

  await appInstance.close()
})

describe('objects - list v2 sorting tests', () => {
  const TEST_CASES = [
    // WITH DELIMITER
    {
      desc: 'with delimiter - default sorting (name asc)',
      options: {
        with_delimiter: true,
      },
      expected: { objects: SORTED_OBJECTS, folders: SORTED_FOLDERS },
    },
    {
      desc: 'with delimiter - name desc',
      options: {
        with_delimiter: true,
        sortBy: {
          column: 'name',
          order: 'desc',
        },
      },
      expected: {
        objects: SORTED_OBJECTS.slice().reverse(),
        folders: SORTED_FOLDERS.slice().reverse(),
      },
    },

    {
      desc: 'with delimiter - created asc',
      options: {
        with_delimiter: true,
        sortBy: {
          column: 'created_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return CREATION_ORDER_OBJECTS
        },
        get folders() {
          return CREATION_ORDER_FOLDERS
        },
      },
    },
    {
      desc: 'with delimiter - created desc',
      options: {
        with_delimiter: true,
        sortBy: {
          column: 'created_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return CREATION_ORDER_OBJECTS.slice().reverse()
        },
        get folders() {
          return CREATION_ORDER_FOLDERS.slice().reverse()
        },
      },
    },

    {
      desc: 'with delimiter - updated asc',
      options: {
        with_delimiter: true,
        sortBy: {
          column: 'updated_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return UPDATE_ORDER_OBJECTS
        },
        get folders() {
          return CREATION_ORDER_FOLDERS
        },
      },
    },
    {
      desc: 'with delimiter - updated desc',
      options: {
        with_delimiter: true,
        sortBy: {
          column: 'updated_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return UPDATE_ORDER_OBJECTS.slice().reverse()
        },
        get folders() {
          return CREATION_ORDER_FOLDERS.slice().reverse()
        },
      },
    },

    // WITHOUT DELIMITER
    {
      desc: 'without delimiter - default sorting (name asc)',
      options: {
        with_delimiter: false,
      },
      expected: { objects: ALL_PATHS, folders: [] },
    },
    {
      desc: 'without delimiter - name desc without delimiter',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'name',
          order: 'desc',
        },
      },
      expected: { objects: ALL_PATHS.slice().reverse(), folders: [] },
    },

    {
      desc: 'without delimiter - created asc',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'created_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return CREATION_ORDER_ALL
        },
        folders: [],
      },
    },
    {
      desc: 'without delimiter - created desc',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'created_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return CREATION_ORDER_ALL.slice().reverse()
        },
        folders: [],
      },
    },

    {
      desc: 'without delimiter - updated asc',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'updated_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return UPDATE_ORDER_ALL
        },
        folders: [],
      },
    },
    {
      desc: 'without delimiter - updated desc',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'updated_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return UPDATE_ORDER_ALL.slice().reverse()
        },
        folders: [],
      },
    },

    // WITH PREFIX
    {
      desc: `prefix - with delimiter - default sorting (name asc)`,
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
      },
      expected: { objects: PREFIX_OBJECTS[TEST_PREFIX + '/'].sorted, folders: [] },
    },
    {
      desc: 'prefix - with delimiter - name desc',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
        sortBy: {
          column: 'name',
          order: 'desc',
        },
      },
      expected: {
        objects: PREFIX_OBJECTS[TEST_PREFIX + '/'].sorted.slice().reverse(),
        folders: [],
      },
    },

    {
      desc: 'prefix - with delimiter - created asc',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
        sortBy: {
          column: 'created_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return PREFIX_OBJECTS[TEST_PREFIX + '/'].created
        },
        folders: [],
      },
    },
    {
      desc: 'prefix - with delimiter - created desc',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
        sortBy: {
          column: 'created_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return PREFIX_OBJECTS[TEST_PREFIX + '/'].created.slice().reverse()
        },
        folders: [],
      },
    },

    {
      desc: 'prefix - with delimiter - updated asc',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
        sortBy: {
          column: 'updated_at',
          order: 'asc',
        },
      },
      expected: {
        get objects() {
          return PREFIX_OBJECTS[TEST_PREFIX + '/'].updated
        },
        folders: [],
      },
    },
    {
      desc: 'prefix - with delimiter - updated desc',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX + '/',
        sortBy: {
          column: 'updated_at',
          order: 'desc',
        },
      },
      expected: {
        get objects() {
          return PREFIX_OBJECTS[TEST_PREFIX + '/'].updated.slice().reverse()
        },
        folders: [],
      },
    },

    {
      desc: 'prefix with slash - without delimiter',
      options: {
        with_delimiter: false,
        prefix: TEST_PREFIX + '/',
      },
      expected: { objects: PREFIX_OBJECTS[TEST_PREFIX + '/'].sorted, folders: [] },
    },

    {
      desc: 'prefix without slash - with delimiter',
      options: {
        with_delimiter: true,
        prefix: TEST_PREFIX,
      },
      expected: { objects: [TEST_PREFIX + '.txt'], folders: [TEST_PREFIX + '/'] },
    },

    {
      desc: 'prefix without slash - without delimiter',
      options: {
        with_delimiter: false,
        prefix: TEST_PREFIX,
      },
      expected: {
        objects: [TEST_PREFIX + '.txt', ...PREFIX_OBJECTS[TEST_PREFIX + '/'].sorted],
        folders: [],
      },
    },
  ]

  for (const { desc, options, expected } of TEST_CASES) {
    test(desc + ' in correct order with pagination', async () => {
      const limit = 5
      let cursor: string | undefined
      let pageCount = 0
      let lastObjectIdx = -1
      let lastFolderIdx = -1
      let hasNext = false

      // Paginate through all results
      do {
        const response = await appInstance.inject({
          method: 'POST',
          url: '/object/list-v2/' + LIST_V2_BUCKET,
          headers: {
            authorization: `Bearer ${serviceKey}`,
          },
          payload: {
            ...options,
            limit,
            cursor,
          },
        })

        const data = response.json<ListObjectsV2Result>()
        expect(response.statusCode).toBe(200)

        // Verify each object is the expected next one in sequence
        data.objects.forEach((obj) => {
          const expObj = expected.objects[++lastObjectIdx]
          expect(obj.name).toBe(expObj)
        })

        // Verify each folder is the expected next one in sequence
        data.folders.forEach((folder) => {
          const expFolder = expected.folders[++lastFolderIdx]
          expect(folder.name).toBe(expFolder)
        })
        pageCount++

        hasNext = data.hasNext ?? false
        if (!hasNext) {
          expect(data.nextCursor).toBeUndefined()
        } else {
          cursor = data.nextCursor as string
          expect(cursor).toBeDefined()
        }
      } while (hasNext)

      // Verify we processed all expected items
      expect(lastObjectIdx).toBe(expected.objects.length - 1)
      expect(lastFolderIdx).toBe(expected.folders.length - 1)
      expect(pageCount).toBe(Math.ceil((expected.objects.length + expected.folders.length) / limit))
    })
  }
})

const LIST_V2_WILDCARD_BUCKET = `list-v2-wildcard-${randomUUID()}`

describe('objects - list v2 prefix wildcard handling', () => {
  beforeAll(async () => {
    appInstance = app()
    await appInstance.inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        name: LIST_V2_WILDCARD_BUCKET,
      },
    })
    await appInstance.close()
  })

  afterAll(async () => {
    appInstance = app()
    await appInstance.inject({
      method: 'POST',
      url: `/bucket/${LIST_V2_WILDCARD_BUCKET}/empty`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    await appInstance.inject({
      method: 'DELETE',
      url: `/bucket/${LIST_V2_WILDCARD_BUCKET}`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    await appInstance.close()
  })

  test('treats % as a literal character in list-v2 prefix filters', async () => {
    const runId = Date.now().toString(36)
    const firstObject = `percent-${runId}/first.txt`
    const secondObject = `percent-${runId}/second.txt`

    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_WILDCARD_BUCKET}/${firstObject}`,
      payload: createUpload('first.txt', 'first'),
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_WILDCARD_BUCKET}/${secondObject}`,
      payload: createUpload('second.txt', 'second'),
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/list-v2/${LIST_V2_WILDCARD_BUCKET}`,
      payload: {
        with_delimiter: false,
        prefix: '%',
        limit: 100,
      },
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    expect(response.statusCode).toBe(200)
    const data = response.json<ListObjectsV2Result>()
    expect(data.folders).toHaveLength(0)
    expect(data.objects).toHaveLength(0)
  })

  test('treats _ as a literal character in list-v2 prefix filters', async () => {
    const runId = randomUUID()
    const literalMatch = `wild_${runId}/hit.txt`
    const wildcardOnlyMatch = `wildX${runId}/miss.txt`

    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_WILDCARD_BUCKET}/${literalMatch}`,
      payload: createUpload('hit.txt', 'hit'),
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_WILDCARD_BUCKET}/${wildcardOnlyMatch}`,
      payload: createUpload('miss.txt', 'miss'),
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/list-v2/${LIST_V2_WILDCARD_BUCKET}`,
      payload: {
        with_delimiter: false,
        prefix: `wild_${runId}/`,
        limit: 100,
      },
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })

    expect(response.statusCode).toBe(200)
    const data = response.json<ListObjectsV2Result>()
    expect(data.folders).toHaveLength(0)
    expect(data.objects.map((obj) => obj.name)).toEqual([literalMatch])
  })
})

describe('objects - list v2 versioning tests', () => {
  function buildVersionRows(
    bucketId: string,
    name: string,
    count: number,
    baseTime: number
  ): Array<Partial<Obj> & { bucket_id: string; name: string }> {
    const rows: Array<Partial<Obj> & { bucket_id: string; name: string }> = []
    for (let i = 0; i < count; i++) {
      const isCurrent = i === count - 1
      const createdAt = new Date(baseTime + i * 1000).toISOString()
      rows.push({
        bucket_id: bucketId,
        name,
        owner: 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2',
        version: `v${i}-${randomUUID()}`,
        metadata: { mimetype: 'image/png', size: 1000 + i },
        created_at: createdAt,
        archived_at: isCurrent ? null : createdAt,
        is_versioned: true,
        is_delete_marker: false,
      })
    }
    return rows
  }

  test('every noncurrentVersions x deleteMarkers combination on listObjectsV2 (flat)', async () => {
    const runId = randomUUID()
    const objectName = `authenticated/tristate-matrix-${runId}.png`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const rows = [
      { version: 'archived-content-1', archivedAt: baseTime, isDeleteMarker: false },
      { version: 'archived-marker', archivedAt: baseTime + 1000, isDeleteMarker: true },
      { version: 'archived-content-2', archivedAt: baseTime + 2000, isDeleteMarker: false },
      { version: 'current-content', archivedAt: null, isDeleteMarker: false },
    ]

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(
      seedTx,
      rows.map((row) => ({
        bucket_id: 'bucket2',
        name: objectName,
        owner: 'd8c7bce9-cfeb-497b-bd61-e66ce2cbdaa2',
        version: row.version,
        metadata: { mimetype: 'image/png', size: 1000 },
        created_at: new Date(row.archivedAt ?? baseTime + 3000).toISOString(),
        archived_at: row.archivedAt === null ? null : new Date(row.archivedAt).toISOString(),
        is_versioned: true,
        is_delete_marker: row.isDeleteMarker,
      }))
    )
    await seedTx.commit()
    tnx = undefined

    try {
      const matrix: {
        noncurrentVersions?: 'exclude' | 'include' | 'only'
        deleteMarkers?: 'exclude' | 'include' | 'only'
        expected: string[]
      }[] = [
        { expected: ['current-content'] },
        {
          noncurrentVersions: 'exclude',
          deleteMarkers: 'exclude',
          expected: ['current-content'],
        },
        { noncurrentVersions: 'exclude', deleteMarkers: 'only', expected: [] },
        {
          noncurrentVersions: 'exclude',
          deleteMarkers: 'include',
          expected: ['current-content'],
        },
        {
          noncurrentVersions: 'only',
          deleteMarkers: 'exclude',
          expected: ['archived-content-1', 'archived-content-2'],
        },
        { noncurrentVersions: 'only', deleteMarkers: 'only', expected: ['archived-marker'] },
        {
          noncurrentVersions: 'only',
          deleteMarkers: 'include',
          expected: ['archived-content-1', 'archived-marker', 'archived-content-2'],
        },
        {
          noncurrentVersions: 'include',
          deleteMarkers: 'exclude',
          expected: ['archived-content-1', 'archived-content-2', 'current-content'],
        },
        {
          noncurrentVersions: 'include',
          deleteMarkers: 'only',
          expected: ['archived-marker'],
        },
        {
          noncurrentVersions: 'include',
          deleteMarkers: 'include',
          expected: [
            'archived-content-1',
            'archived-marker',
            'archived-content-2',
            'current-content',
          ],
        },
      ]

      for (const { noncurrentVersions, deleteMarkers, expected } of matrix) {
        const response = await appInstance.inject({
          method: 'POST',
          url: '/object/list-v2/bucket2',
          headers: { authorization: `Bearer ${await serviceKeyAsync}` },
          payload: { prefix: objectName, exactMatch: true, noncurrentVersions, deleteMarkers },
        })
        expect(response.statusCode).toBe(200)
        const body = response.json<{ objects: Obj[] }>()
        const versions = body.objects.map((o) => o.version)
        expect(new Set(versions)).toEqual(new Set(expected))
      }
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', objectName)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('most-recent-first ordering within a key on listObjectsV2 (flat, noncurrentVersions include)', async () => {
    const runId = randomUUID()
    const objectName = `authenticated/versions-order-${runId}.png`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, buildVersionRows('bucket2', objectName, 3, baseTime))
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/object/list-v2/bucket2',
        headers: { authorization: `Bearer ${await serviceKeyAsync}` },
        payload: { prefix: objectName, exactMatch: true, noncurrentVersions: 'include' },
      })
      expect(response.statusCode).toBe(200)
      const body = response.json<{ objects: Obj[] }>()
      expect(body.objects).toHaveLength(3)
      expect(body.objects[0].archived_at).toBeNull()
      expect(body.objects[0].version?.startsWith('v2-')).toBe(true)
      expect(body.objects[1].version?.startsWith('v1-')).toBe(true)
      expect(body.objects[2].version?.startsWith('v0-')).toBe(true)
      const versions = body.objects.map((o) => o.version)
      expect(new Set(versions).size).toBe(3)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', objectName)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test("pagination does not repeat a key's current row when a page boundary lands right after it (with delimiter)", async () => {
    const runId = randomUUID()
    const objectName = `authenticated/current-row-boundary-${runId}.png`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, buildVersionRows('bucket2', objectName, 3, baseTime))
    await seedTx.commit()
    tnx = undefined

    try {
      const seen: string[] = []
      let cursor: string | undefined
      let pages = 0

      do {
        const response = await appInstance.inject({
          method: 'POST',
          url: '/object/list-v2/bucket2',
          headers: { authorization: `Bearer ${await serviceKeyAsync}` },
          payload: {
            prefix: objectName,
            exactMatch: true,
            with_delimiter: true,
            noncurrentVersions: 'include',
            limit: 1,
            cursor,
          },
        })
        expect(response.statusCode).toBe(200)
        const body = response.json<{ objects: Obj[]; hasNext: boolean; nextCursor?: string }>()

        for (const o of body.objects) {
          seen.push(o.version as string)
        }

        cursor = body.hasNext ? body.nextCursor : undefined
        pages += 1
        expect(pages).toBeLessThan(10)
      } while (cursor)

      expect(seen).toHaveLength(3)
      expect(new Set(seen).size).toBe(3)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', objectName)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('exactMatch on listObjectsV2 matches only the literal key, not a shared prefix', async () => {
    const runId = randomUUID()
    const objectName = `authenticated/exact-match-${runId}.png`
    const decoyName = `${objectName}.decoy`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, [
      ...buildVersionRows('bucket2', objectName, 2, baseTime),
      ...buildVersionRows('bucket2', decoyName, 1, baseTime),
    ])
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/object/list-v2/bucket2',
        headers: { authorization: `Bearer ${await serviceKeyAsync}` },
        payload: {
          prefix: objectName,
          exactMatch: true,
          noncurrentVersions: 'include',
        },
      })
      expect(response.statusCode).toBe(200)
      const body = response.json<{ objects: Obj[] }>()
      expect(body.objects).toHaveLength(2)
      expect(body.objects.every((o) => o.name === objectName)).toBe(true)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', [objectName, decoyName])
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('pagination across a multi-version key boundary returns every version exactly once (flat)', async () => {
    const runId = randomUUID()
    const prefix = `authenticated/pagination-boundary-${runId}`
    const keyA = `${prefix}-a.png`
    const keyB = `${prefix}-b-boundary.png`
    const keyC = `${prefix}-c.png`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, [
      ...buildVersionRows('bucket2', keyA, 1, baseTime),
      ...buildVersionRows('bucket2', keyB, 5, baseTime + 100_000),
      ...buildVersionRows('bucket2', keyC, 1, baseTime + 200_000),
    ])
    await seedTx.commit()
    tnx = undefined

    try {
      const seen: { name: string; version: string }[] = []
      let cursor: string | undefined
      let pages = 0

      do {
        const response = await appInstance.inject({
          method: 'POST',
          url: '/object/list-v2/bucket2',
          headers: { authorization: `Bearer ${await serviceKeyAsync}` },
          payload: {
            prefix,
            noncurrentVersions: 'include',
            limit: 3,
            cursor,
          },
        })
        expect(response.statusCode).toBe(200)
        const body = response.json<{
          objects: Obj[]
          hasNext: boolean
          nextCursor?: string
        }>()

        for (const o of body.objects) {
          seen.push({ name: o.name, version: o.version as string })
        }

        cursor = body.hasNext ? body.nextCursor : undefined
        pages += 1
        expect(pages).toBeLessThan(10)
      } while (cursor)

      expect(pages).toBeGreaterThan(1)

      const seenKeys = seen.map((o) => `${o.name}::${o.version}`)
      expect(new Set(seenKeys).size).toBe(seenKeys.length)
      expect(seen).toHaveLength(7) // 1 (a) + 5 (b) + 1 (c)

      const bVersions = seen.filter((o) => o.name === keyB)
      expect(bVersions).toHaveLength(5)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', [keyA, keyB, keyC])
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('pagination across a multi-version key boundary returns every version exactly once (with delimiter)', async () => {
    const runId = randomUUID()
    const prefix = `authenticated/pagination-boundary-delim-${runId}`
    const keyA = `${prefix}-a.png`
    const keyB = `${prefix}-b-boundary.png`
    const keyC = `${prefix}-c.png`
    const baseTime = Date.parse('2024-01-01T00:00:00.000Z')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, [
      ...buildVersionRows('bucket2', keyA, 1, baseTime),
      ...buildVersionRows('bucket2', keyB, 5, baseTime + 100_000),
      ...buildVersionRows('bucket2', keyC, 1, baseTime + 200_000),
    ])
    await seedTx.commit()
    tnx = undefined

    try {
      const seen: { name: string; version: string }[] = []
      let cursor: string | undefined
      let pages = 0

      do {
        const response = await appInstance.inject({
          method: 'POST',
          url: '/object/list-v2/bucket2',
          headers: { authorization: `Bearer ${await serviceKeyAsync}` },
          payload: {
            prefix,
            with_delimiter: true,
            noncurrentVersions: 'include',
            limit: 3,
            cursor,
          },
        })
        expect(response.statusCode).toBe(200)
        const body = response.json<{
          objects: Obj[]
          hasNext: boolean
          nextCursor?: string
        }>()

        for (const o of body.objects) {
          seen.push({ name: o.name, version: o.version as string })
        }

        cursor = body.hasNext ? body.nextCursor : undefined
        pages += 1
        expect(pages).toBeLessThan(10)
      } while (cursor)

      expect(pages).toBeGreaterThan(1)

      const seenKeys = seen.map((o) => `${o.name}::${o.version}`)
      expect(new Set(seenKeys).size).toBe(seenKeys.length)
      expect(seen).toHaveLength(7)

      const bVersions = seen.filter((o) => o.name === keyB)
      expect(bVersions).toHaveLength(5)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, 'bucket2', [keyA, keyB, keyC])
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })
})
