'use strict'

import app from '../app'
import { getConfig } from '../config'
import { useMockObject, useMockQueue } from './common'
import { Knex } from 'knex'
import { FastifyInstance } from 'fastify'
import { ListObjectsV2Result } from '@storage/object'

const { serviceKeyAsync } = getConfig()
let appInstance: FastifyInstance
let serviceKey: string = ''

let tnx: Knex.Transaction | undefined

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

// Helper to convert a number into a 3-letter string (aaa ... zzz)
const toName = (n: number): string => {
  const a = 97 // 'a'
  const first = String.fromCharCode(a + (Math.floor(n / (26 * 26)) % 26))
  const second = String.fromCharCode(a + (Math.floor(n / 26) % 26))
  const third = String.fromCharCode(a + (n % 26))
  return first + second + third
}

// Statically created sorted list of file paths
// 20 objects (.txt extension) and 20 folders (no extension) - already sorted
const SORTED_OBJECTS: string[] = []
const SORTED_FOLDERS: string[] = []
const NESTED_OBJECTS: string[] = []

// Generate sorted list of objects/folders
for (let i = 0; i < 30; i++) {
  if (i > 5) {
    SORTED_OBJECTS.push(toName(i) + '.txt')
  }
  if (i < 18) {
    const folder = toName(i) + '/'
    SORTED_FOLDERS.push(folder)

    for (let j = 0; j < 3; j++) {
      const objectPath = `${folder}dummy${j}.txt`
      NESTED_OBJECTS.push(objectPath)
    }
  }
}

// Combine all paths for creation
const ALL_PATHS = [...SORTED_OBJECTS, ...NESTED_OBJECTS].sort()

const UPDATE_ORDER_OBJECTS: string[] = []
const CREATION_ORDER_OBJECTS: string[] = []
const CREATION_ORDER_FOLDERS: string[] = []
const CREATION_ORDER_ALL: string[] = []

beforeAll(async () => {
  serviceKey = await serviceKeyAsync
  appInstance = app()

  // TODO: remove this, not needed once cleanup is uncommented
  // empty if it already exists
  await appInstance.inject({
    method: 'POST',
    url: `/bucket/${LIST_V2_BUCKET}/empty`,
    headers: {
      authorization: `Bearer ${serviceKey}`,
    },
  })

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

  function createUpload(name: string, content: string) {
    return new File([content], name)
  }

  // Shuffle array to create objects in semi-random order (so created_at != name order)
  function shuffleArray<T>(array: T[]): T[] {
    const shuffled = [...array]
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1))
      ;[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]]
    }
    return shuffled
  }

  const shuffledPaths = shuffleArray(ALL_PATHS)

  // Create all objects in random order
  for (const path of shuffledPaths) {
    if (path.includes('/')) {
      // nested objects
      CREATION_ORDER_FOLDERS.push(path.split('/')[0])
    } else {
      // root objects
      CREATION_ORDER_OBJECTS.push(path)
      UPDATE_ORDER_OBJECTS.push(path)
    }
    CREATION_ORDER_ALL.push(path)
    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_BUCKET}/${path}`,
      payload: createUpload(path, 'test content'),
      headers: {
        authorization: serviceKey,
      },
    })
  }

  // update a few objects to make updated_at different than created_at
  for (let i = 0; i < 10; i++) {
    const firstItem = UPDATE_ORDER_OBJECTS.shift()!
    const headers = {
      authorization: serviceKey,
      'x-upsert': 'true',
    }
    await appInstance.inject({
      method: 'POST',
      url: `/object/${LIST_V2_BUCKET}/${firstItem}`,
      payload: createUpload(firstItem, 'test content'),
      headers,
    })
    UPDATE_ORDER_OBJECTS.push(firstItem)
  }

  await appInstance.close()
}, 300000)

// TODO... uncomment this to cleanup after tests
// commented for now so I can do manual tests / debugging against test data
// afterAll(async () => {
//   appInstance = app()

//   // Empty the bucket
//   await appInstance.inject({
//     method: 'POST',
//     url: `/bucket/${LIST_V2_BUCKET}/empty`,
//     headers: {
//       authorization: `Bearer ${serviceKey}`,
//     },
//   })

//   // Delete the bucket
//   await appInstance.inject({
//     method: 'DELETE',
//     url: `/bucket/${LIST_V2_BUCKET}`,
//     headers: {
//       authorization: `Bearer ${serviceKey}`,
//     },
//   })

//   await appInstance.close()
// })

describe('objects - list v2 sorting tests', () => {
  const TEST_CASES = [
    {
      desc: 'default sorting (name asc) with delmiter',
      options: {
        with_delimiter: true,
      },
      expected: { objects: SORTED_OBJECTS, folders: SORTED_FOLDERS },
    },
    {
      desc: 'name desc with delmiter',
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
      desc: 'creation asc with delmiter',
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
        // folders: CREATION_ORDER_FOLDERS, // folders do not have created at so they're sorted alpha
        folders: SORTED_FOLDERS,
      },
    },
    {
      desc: 'creation desc with delmiter',
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
        // folders: CREATION_ORDER_FOLDERS, // folders do not have created at so they're sorted alpha
        folders: SORTED_FOLDERS.slice().reverse(),
      },
    },

    {
      desc: 'creation asc with delmiter',
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
        // folders: CREATION_ORDER_FOLDERS, // folders do not have created at so they're sorted alpha
        folders: SORTED_FOLDERS,
      },
    },
    {
      desc: 'creation desc with delmiter',
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
        // folders: CREATION_ORDER_FOLDERS, // folders do not have created at so they're sorted alpha
        folders: SORTED_FOLDERS.slice().reverse(),
      },
    },

    {
      desc: 'default sorting (name asc) without delimiter',
      options: {
        with_delimiter: false,
      },
      expected: { objects: ALL_PATHS, folders: [] },
    },
    {
      desc: 'name desc without delimiter',
      options: {
        with_delimiter: false,
        sortBy: {
          column: 'name',
          order: 'desc',
        },
      },
      expected: { objects: ALL_PATHS.slice().reverse(), folders: [] },
    },
  ]

  for (let { desc, options, expected } of TEST_CASES) {
    test(desc + ' in correct order with pagination', async () => {
      const limit = 5
      let cursor: string | undefined = undefined
      let pageCount = 0
      let lastObjectIdx = -1
      let lastFolderIdx = -1

      // Paginate through all results
      while (true) {
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

        if (!data.hasNext) {
          expect(data.nextCursor).toBeUndefined()
          break
        }

        cursor = data.nextCursor as string
        expect(cursor).toBeDefined()
      }

      // Verify we processed all expected items
      expect(lastObjectIdx).toBe(expected.objects.length - 1)
      expect(lastFolderIdx).toBe(expected.folders.length - 1)
      expect(pageCount).toBe(Math.ceil((expected.objects.length + expected.folders.length) / limit))
    })
  }
})
