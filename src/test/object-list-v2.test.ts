import { randomUUID } from 'node:crypto'
import { ListObjectsV2Result } from '@storage/object'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig } from '../config'
import { useMockObject, useMockQueue } from './common'
import { useStorage } from './utils/storage'

const { serviceKeyAsync } = getConfig()
let appInstance: FastifyInstance
let serviceKey: string = ''

useMockObject()
useMockQueue()
const storageTest = useStorage()

beforeEach(() => {
  getConfig({ reload: true })
  appInstance = app()
})

afterEach(async () => {
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

  test('omits default version filters from continuation tokens for rolling-deploy compatibility', async () => {
    const firstPage = await appInstance.inject({
      method: 'POST',
      url: `/object/list-v2/${LIST_V2_BUCKET}`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        with_delimiter: false,
        limit: 1,
      },
    })

    expect(firstPage.statusCode).toBe(200)
    const cursor = firstPage.json<ListObjectsV2Result>().nextCursor
    expect(cursor).toBeDefined()
    const decodedCursor = Buffer.from(cursor!, 'base64').toString()
    expect(decodedCursor).not.toMatch(/(^|\n)n:/)
    expect(decodedCursor).not.toMatch(/(^|\n)d:/)

    const changedFilter = await appInstance.inject({
      method: 'POST',
      url: `/object/list-v2/${LIST_V2_BUCKET}`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        with_delimiter: false,
        limit: 1,
        cursor,
        noncurrentVersions: 'include',
      },
    })

    expect(changedFilter.statusCode).toBe(400)
  })

  test('encodes only non-default version filters in continuation tokens', async () => {
    const firstPage = await appInstance.inject({
      method: 'POST',
      url: `/object/list-v2/${LIST_V2_BUCKET}`,
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        with_delimiter: false,
        limit: 1,
        noncurrentVersions: 'include',
        deleteMarkers: 'exclude',
      },
    })

    expect(firstPage.statusCode).toBe(200)
    const cursor = firstPage.json<ListObjectsV2Result>().nextCursor
    expect(cursor).toBeDefined()
    const decodedCursor = Buffer.from(cursor!, 'base64').toString()
    expect(decodedCursor).toMatch(/(^|\n)n:include($|\n)/)
    expect(decodedCursor).not.toMatch(/(^|\n)d:/)
  })

  test('uses millisecond precision consistently when paginating by timestamp', async () => {
    const prefix = `same-ms-${randomUUID()}/`
    const firstByName = `${prefix}a.txt`
    const secondByName = `${prefix}z.txt`

    for (const path of [firstByName, secondByName]) {
      const upload = await appInstance.inject({
        method: 'POST',
        url: `/object/${LIST_V2_BUCKET}/${path}`,
        payload: createUpload(path, 'test content'),
        headers: {
          authorization: serviceKey,
        },
      })
      expect(upload.statusCode).toBe(200)
    }

    // Raw timestamp ordering puts z.txt first, while millisecond-truncated
    // ordering treats the timestamps as equal and uses name as the tiebreak.
    // The ORDER BY and cursor comparison must use the same representation.
    await storageTest.database.connection.query(
      `UPDATE storage.objects
       SET created_at = CASE name
         WHEN $2 THEN '2026-01-01 00:00:00.000900+00'::timestamptz
         WHEN $3 THEN '2026-01-01 00:00:00.000100+00'::timestamptz
       END
       WHERE bucket_id = $1 AND name IN ($2, $3)`,
      [LIST_V2_BUCKET, firstByName, secondByName]
    )

    const listed: string[] = []
    let cursor: string | undefined
    do {
      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/list-v2/${LIST_V2_BUCKET}`,
        headers: {
          authorization: `Bearer ${serviceKey}`,
        },
        payload: {
          with_delimiter: false,
          prefix,
          limit: 1,
          cursor,
          sortBy: { column: 'created_at', order: 'asc' },
        },
      })

      expect(response.statusCode).toBe(200)
      const data = response.json<ListObjectsV2Result>()
      listed.push(...data.objects.map((object) => object.name))
      cursor = data.nextCursor
    } while (cursor)

    expect(listed).toEqual([firstByName, secondByName])
  })
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

    for (const withDelimiter of [false, true]) {
      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/list-v2/${LIST_V2_WILDCARD_BUCKET}`,
        payload: {
          with_delimiter: withDelimiter,
          prefix: '%',
          limit: 100,
          sortBy: { column: 'created_at', order: 'asc' },
        },
        headers: {
          authorization: `Bearer ${serviceKey}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const data = response.json<ListObjectsV2Result>()
      expect(data.folders).toHaveLength(0)
      expect(data.objects).toHaveLength(0)
    }
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

    for (const withDelimiter of [false, true]) {
      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/list-v2/${LIST_V2_WILDCARD_BUCKET}`,
        payload: {
          with_delimiter: withDelimiter,
          prefix: `wild_${runId}/`,
          limit: 100,
          sortBy: { column: 'created_at', order: 'asc' },
        },
        headers: {
          authorization: `Bearer ${serviceKey}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const data = response.json<ListObjectsV2Result>()
      expect(data.folders).toHaveLength(0)
      expect(data.objects.map((obj) => obj.name)).toEqual([literalMatch])
    }
  })
})

describe('objects - list v2 startAfter and folder cursors', () => {
  async function insertPaths(paths: string[]) {
    await storageTest.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name)
       SELECT $1, object_name FROM unnest($2::text[]) AS object_name`,
      [LIST_V2_BUCKET, paths]
    )
  }

  test('treats wildcard characters literally when inferring a startAfter folder', async () => {
    const runId = randomUUID()
    const prefix = `start-after-wildcard-${runId}/`
    const boundary = `${prefix}aa_%`
    const adjacentName = `${boundary}!file.txt`
    const wildcardOnlyChild = `${prefix}aaX/child.txt`
    await insertPaths([adjacentName, wildcardOnlyChild])

    const result = await storageTest.storage.from(LIST_V2_BUCKET).listObjectsV2({
      prefix,
      delimiter: '/',
      startAfter: boundary,
      maxKeys: 10,
      sortBy: { column: 'name', order: 'asc' },
    })

    expect(result.objects.map((object) => object.name)).toEqual([adjacentName])
    expect(result.folders.map((folder) => folder.name)).toEqual([`${prefix}aaX/`])
  })

  test('does not move backward when an inferred folder startAfter omits the delimiter', async () => {
    const runId = randomUUID()
    const prefix = `start-after-folder-${runId}/`
    const earlierName = `${prefix}za.txt`
    const boundary = `${prefix}zz`
    const childName = `${boundary}/child.txt`
    const laterName = `${prefix}zzz.txt`
    await insertPaths([earlierName, childName, laterName])

    const result = await storageTest.storage.from(LIST_V2_BUCKET).listObjectsV2({
      prefix,
      delimiter: '/',
      startAfter: boundary,
      maxKeys: 10,
      sortBy: { column: 'name', order: 'asc' },
    })

    expect(result.objects.map((object) => object.name)).toEqual([laterName])
    expect(result.folders).toEqual([])
  })

  test.each([
    ['created_at', 'asc', ['aa!', 'aa/', 'ab']],
    ['created_at', 'desc', ['ab', 'aa/', 'aa!']],
    ['updated_at', 'asc', ['aa!', 'aa/', 'ab']],
    ['updated_at', 'desc', ['ab', 'aa/', 'aa!']],
  ] as const)('keeps folder names and timestamps in %s %s pagination cursors', async (column, order, expected) => {
    const runId = randomUUID()
    const prefix = `timestamp-folder-${runId}/`
    const adjacentName = `${prefix}aa!`
    const childName = `${prefix}aa/child.txt`
    const laterName = `${prefix}ab`
    await storageTest.database.connection.query(
      `INSERT INTO storage.objects (bucket_id, name, created_at, updated_at)
         SELECT $1, object_name, $3::timestamptz, $3::timestamptz
         FROM unnest($2::text[]) AS object_name`,
      [LIST_V2_BUCKET, [adjacentName, childName, laterName], '2024-11-01T00:00:00.000Z']
    )

    const actual: string[] = []
    let cursor: string | undefined

    do {
      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/list-v2/${LIST_V2_BUCKET}`,
        headers: { authorization: `Bearer ${serviceKey}` },
        payload: {
          prefix,
          with_delimiter: true,
          limit: 1,
          cursor,
          sortBy: { column, order },
        },
      })
      expect(response.statusCode).toBe(200)

      const body = response.json<ListObjectsV2Result>()
      expect(body.objects.length + body.folders.length).toBeLessThanOrEqual(1)
      actual.push(...body.objects.map((object) => object.name))
      actual.push(...body.folders.map((folder) => folder.name))
      cursor = body.hasNext ? body.nextCursor : undefined
      expect(actual.length).toBeLessThan(10)
    } while (cursor)

    expect(actual).toEqual(expected.map((name) => `${prefix}${name}`))
  })
})
