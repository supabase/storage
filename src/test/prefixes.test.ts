'use strict'

import app from '../app'
import { getConfig } from '../config'
import { useMockObject, useMockQueue } from './common'
import { FastifyInstance } from 'fastify'
import { useStorage } from './utils/storage'

const { serviceKeyAsync } = getConfig()

let appInstance: FastifyInstance

useMockObject()
useMockQueue()

describe('Prefix Hierarchy Race Condition Tests', () => {
  const bucketName = `test-prefixes-${Date.now()}`

  const tHelper = useStorage()

  beforeAll(async () => {
    getConfig({ reload: true })
    appInstance = app()

    // Create test bucket
    const response = await appInstance.inject({
      method: 'POST',
      url: '/bucket',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        name: bucketName,
      },
    })

    if (response.statusCode !== 200) {
      console.error('Failed to create bucket:', response.body)
    }
  })

  afterAll(async () => {
    await appInstance.close()
    await tHelper.database.connection.dispose()
  })

  afterEach(async () => {
    // Clean up any existing prefixes before each test
    try {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('DELETE FROM storage.objects WHERE bucket_id = ?', [bucketName])
      await db.raw('DELETE FROM storage.prefixes WHERE bucket_id = ?', [bucketName])
    } catch (error) {
      console.log('Cleanup error in beforeEach:', error)
    }
  })

  // Helper function to create objects
  async function createObject(objectName: string, content = 'test content') {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/${bucketName}/${objectName}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'Content-Type': 'text/plain',
      },
      payload: content,
    })
    expect(response.statusCode).toBe(200)
    return response.json()
  }

  // Helper function to delete objects
  async function deleteObjects(prefixes: string[]) {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: `/object/${bucketName}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        prefixes,
      },
    })
    expect(response.statusCode).toBe(200)
    return response.json()
  }

  // Helper function to move objects
  async function moveObject(source: string, destination: string, upsert = false) {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/move',
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'Content-Type': 'application/json',
      },
      payload: {
        bucketId: bucketName,
        sourceKey: source,
        destinationKey: destination,
        upsert,
      },
    })
    expect(response.statusCode).toBe(200)
    return response.json()
  }

  // Helper function to check prefixes in database
  async function getPrefixes(): Promise<Array<{ bucket_id: string; name: string; level: number }>> {
    const db = tHelper.database.connection.pool.acquire()
    const prefixes = await db
      .select('bucket_id', 'name', 'level')
      .from('storage.prefixes')
      .where('bucket_id', bucketName)
      .orderBy('level')
      .orderBy('name')

    return prefixes
  }

  describe('Basic Prefix Cleanup', () => {
    it('should create prefixes when objects are created', async () => {
      await createObject('folder/subfolder/file.txt')

      const prefixes = await getPrefixes()
      expect(prefixes).toEqual([
        { bucket_id: bucketName, name: 'folder', level: 1 },
        { bucket_id: bucketName, name: 'folder/subfolder', level: 2 },
      ])
    })

    it('should cleanup prefixes when all objects in a folder are deleted', async () => {
      // Create objects in nested folders
      await createObject('folder/subfolder/file1.txt')
      await createObject('folder/subfolder/file2.txt')
      await createObject('folder/other/file3.txt')

      // Check initial prefixes
      let prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'folder', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'folder/subfolder', level: 2 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'folder/other', level: 2 })

      // Delete all files in subfolder
      await deleteObjects(['folder/subfolder/file1.txt', 'folder/subfolder/file2.txt'])

      // folder/subfolder should be gone, but folder and folder/other should remain
      prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'folder', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'folder/other', level: 2 })
      expect(prefixes).not.toContainEqual({
        bucket_id: bucketName,
        name: 'folder/subfolder',
        level: 2,
      })

      // Delete last file in folder
      await deleteObjects(['folder/other/file3.txt'])

      // All prefixes should be gone
      prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })
  })

  describe('Race Condition Scenario 1: Concurrent Deletes of Related Objects', () => {
    it('should handle concurrent deletion of objects in same folder without leaving dangling prefixes', async () => {
      // Create multiple objects in the same folder structure
      await createObject('shared/folder/file1.txt')
      await createObject('shared/folder/file2.txt')
      await createObject('shared/folder/file3.txt')
      await createObject('shared/folder/file4.txt')

      // Verify prefixes were created
      let prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'shared', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'shared/folder', level: 2 })

      // Simulate concurrent deletion by deleting multiple objects at once
      // This tests the race condition where multiple triggers fire for the same prefix
      const deletePromises = [
        deleteObjects(['shared/folder/file1.txt']),
        deleteObjects(['shared/folder/file2.txt']),
        deleteObjects(['shared/folder/file3.txt']),
        deleteObjects(['shared/folder/file4.txt']),
      ]

      const results = await Promise.all(deletePromises)

      // Verify all objects were deleted
      const totalDeleted = results.reduce((sum, result) => sum + result.length, 0)
      expect(totalDeleted).toBe(4)

      // Most importantly: verify no dangling prefixes remain
      prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should handle partial concurrent deletion correctly', async () => {
      // Create objects in multiple subfolders
      await createObject('race/test/file1.txt')
      await createObject('race/test/file2.txt')
      await createObject('race/other/file3.txt')

      // Delete objects from one subfolder concurrently
      const deletePromises = [
        deleteObjects(['race/test/file1.txt']),
        deleteObjects(['race/test/file2.txt']),
      ]

      await Promise.all(deletePromises)

      // race/test should be gone, but race and race/other should remain
      const prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'race', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'race/other', level: 2 })
      expect(prefixes).not.toContainEqual({ bucket_id: bucketName, name: 'race/test', level: 2 })
    })
  })

  describe('Race Condition Scenario 2: Batch Deletes', () => {
    it('should handle batch deletion of multiple objects without race conditions', async () => {
      // Create a complex folder structure
      const objectNames = [
        'batch/level1/file1.txt',
        'batch/level1/file2.txt',
        'batch/level2/sub1/file3.txt',
        'batch/level2/sub1/file4.txt',
        'batch/level2/sub2/file5.txt',
        'batch/level3/deep/nested/file6.txt',
      ]

      // Create all objects
      for (const name of objectNames) {
        await createObject(name)
      }

      // Verify complex prefix structure
      let prefixes = await getPrefixes()
      const expectedPrefixes = [
        'batch',
        'batch/level1',
        'batch/level2',
        'batch/level2/sub1',
        'batch/level2/sub2',
        'batch/level3',
        'batch/level3/deep',
        'batch/level3/deep/nested',
      ]

      for (const expectedPrefix of expectedPrefixes) {
        expect(prefixes.map((p) => p.name)).toContain(expectedPrefix)
      }

      // Delete all objects in a single batch operation
      // This tests the race condition within a single transaction
      const result = await deleteObjects(objectNames)
      expect(result).toHaveLength(6)

      // All prefixes should be cleaned up
      prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should handle partial batch deletion correctly', async () => {
      // Create objects across multiple folder structures
      await createObject('partial/keep/file1.txt')
      await createObject('partial/delete/file2.txt')
      await createObject('partial/delete/file3.txt')
      await createObject('partial/mixed/keep.txt')
      await createObject('partial/mixed/delete.txt')

      // Delete only some objects in batch
      const result = await deleteObjects([
        'partial/delete/file2.txt',
        'partial/delete/file3.txt',
        'partial/mixed/delete.txt',
      ])
      expect(result).toHaveLength(3)

      // Verify correct prefixes remain
      const prefixes = await getPrefixes()
      const remainingPrefixNames = prefixes.map((p) => p.name)

      // These should remain
      expect(remainingPrefixNames).toContain('partial')
      expect(remainingPrefixNames).toContain('partial/keep')
      expect(remainingPrefixNames).toContain('partial/mixed')

      // This should be gone
      expect(remainingPrefixNames).not.toContain('partial/delete')
    })
  })

  describe('Deep Hierarchy Cleanup', () => {
    it('should recursively clean up deep prefix hierarchies', async () => {
      // Create a very deep hierarchy
      await createObject('deep/level1/level2/level3/level4/level5/file.txt')

      // Verify all prefixes were created
      let prefixes = await getPrefixes()
      const expectedLevels = [
        'deep',
        'deep/level1',
        'deep/level1/level2',
        'deep/level1/level2/level3',
        'deep/level1/level2/level3/level4',
        'deep/level1/level2/level3/level4/level5',
      ]

      for (const expected of expectedLevels) {
        expect(prefixes.map((p) => p.name)).toContain(expected)
      }

      // Delete the single file - should trigger recursive cleanup
      const result = await deleteObjects(['deep/level1/level2/level3/level4/level5/file.txt'])
      expect(result).toHaveLength(1)

      // All prefixes should be recursively cleaned up
      prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should stop recursive cleanup when other objects exist at intermediate levels', async () => {
      // Create deep hierarchy with objects at different levels
      await createObject('stop/level1/file_at_level1.txt')
      await createObject('stop/level1/level2/level3/deep_file.txt')

      // Delete only the deep file
      await deleteObjects(['stop/level1/level2/level3/deep_file.txt'])

      // level2 and level3 should be cleaned up, but level1 and stop should remain
      const prefixes = await getPrefixes()
      const remainingNames = prefixes.map((p) => p.name)

      expect(remainingNames).toContain('stop')
      expect(remainingNames).toContain('stop/level1')
      expect(remainingNames).not.toContain('stop/level1/level2')
      expect(remainingNames).not.toContain('stop/level1/level2/level3')
    })
  })

  describe('Edge Cases', () => {
    it('should handle deletion of non-existent objects gracefully', async () => {
      // Create some objects
      await createObject('edge/existing/file.txt')

      // Try to delete mix of existing and non-existing objects
      const result = await deleteObjects([
        'edge/existing/file.txt',
        'edge/nonexistent/file.txt',
        'completely/fake/path.txt',
      ])

      // Only existing object should be in result
      expect(result).toHaveLength(1)
      expect(result[0].name).toBe('edge/existing/file.txt')

      // All prefixes should be cleaned up since the only real object was deleted
      const prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should handle empty folder scenarios', async () => {
      // Create nested structure
      await createObject('empty/test/level1/file1.txt')
      await createObject('empty/test/level2/file2.txt')

      // Delete all files, leaving "empty folders"
      await deleteObjects(['empty/test/level1/file1.txt', 'empty/test/level2/file2.txt'])

      // All prefixes should be cleaned up
      const prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should handle root level files correctly', async () => {
      // Mix of root level and nested files
      await createObject('root_file.txt')
      await createObject('folder/nested_file.txt')

      let prefixes = await getPrefixes()
      // Only folder prefix should exist (root files don't create prefixes)
      expect(prefixes).toHaveLength(1)
      expect(prefixes[0].name).toBe('folder')

      // Delete nested file
      await deleteObjects(['folder/nested_file.txt'])

      // Folder prefix should be gone
      prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)

      // Delete root file (should work fine)
      const result = await deleteObjects(['root_file.txt'])
      expect(result).toHaveLength(1)
    })
  })

  describe('Stress Test: High Concurrency', () => {
    it('should handle many concurrent operations without corruption', async () => {
      // Create many objects in overlapping folder structures
      const objects: string[] = []
      const folders = ['stress1', 'stress2', 'stress3']
      const subfolders = ['sub1', 'sub2', 'sub3']

      for (const folder of folders) {
        for (const subfolder of subfolders) {
          for (let i = 0; i < 5; i++) {
            objects.push(`${folder}/${subfolder}/file${i}.txt`)
          }
        }
      }

      // Create all objects
      for (const obj of objects) {
        await createObject(obj)
      }

      // Verify initial state
      let prefixes = await getPrefixes()
      expect(prefixes.length).toBeGreaterThan(0)

      // Delete all objects with high concurrency
      const batchSize = 3
      const deletePromises: Promise<any>[] = []

      for (let i = 0; i < objects.length; i += batchSize) {
        const batch = objects.slice(i, i + batchSize)
        deletePromises.push(deleteObjects(batch))
      }

      const results = await Promise.all(deletePromises)
      const totalDeleted = results.reduce((sum, result) => sum + result.length, 0)
      expect(totalDeleted).toBe(objects.length)

      const db = tHelper.database.connection.pool.acquire()

      // Final state: no prefixes should remain
      prefixes = await getPrefixes()
      if (prefixes.length > 0) {
        console.log('Dangling prefixes found:', prefixes)

        // Let's check what objects still exist for these prefixes
        for (const prefix of prefixes) {
          const childObjects = await db
            .select('name')
            .from('storage.objects')
            .where('bucket_id', prefix.bucket_id)
            .where('name', 'like', `${prefix.name}/%`)

          const childPrefixes = await db
            .select('name')
            .from('storage.prefixes')
            .where('bucket_id', prefix.bucket_id)
            .where('level', prefix.level + 1)
            .where('name', 'like', `${prefix.name}/%`)

          if (childObjects.length > 0) {
            console.log(
              'Child objects:',
              childObjects.map((o) => o.name)
            )
          }
          if (childPrefixes.length > 0) {
            console.log(
              'Child prefixes:',
              childPrefixes.map((p) => p.name)
            )
          }
        }
      }
      expect(prefixes).toHaveLength(0)
    }, 30000) // Longer timeout for stress test
  })

  describe('Move Operation: Prefix Updates', () => {
    it('should create destination prefixes and cleanup source prefixes when last file leaves a folder', async () => {
      await createObject('move/src/a.txt')
      await createObject('move/src/b.txt')

      // After creation: prefixes should include move and move/src
      let prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move/src', level: 2 })

      // Move first file to a new folder
      await moveObject('move/src/a.txt', 'move/dst/a.txt')

      prefixes = await getPrefixes()
      // Both src and dst should exist (b.txt still in src)
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move/src', level: 2 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move/dst', level: 2 })

      // Move second file, src should be cleaned up
      await moveObject('move/src/b.txt', 'move/dst/b.txt')

      prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'move/dst', level: 2 })
      expect(prefixes).not.toContainEqual({ bucket_id: bucketName, name: 'move/src', level: 2 })
    })

    it('should recursively cleanup old deep prefixes and create new destination prefixes', async () => {
      await createObject('mvdeep/l1/l2/l3/file.txt')

      // Verify source chain exists
      let prefixes = await getPrefixes()
      expect(prefixes.map((p) => p.name)).toEqual(
        expect.arrayContaining(['mvdeep', 'mvdeep/l1', 'mvdeep/l1/l2', 'mvdeep/l1/l2/l3'])
      )

      // Move deep file to a new deep destination
      await moveObject('mvdeep/l1/l2/l3/file.txt', 'mvdeep/other/place/file.txt')

      prefixes = await getPrefixes()
      const names = prefixes.map((p) => p.name)
      // Old deep chain should be cleaned except common root
      expect(names).toContain('mvdeep')
      expect(names).not.toContain('mvdeep/l1')
      expect(names).not.toContain('mvdeep/l1/l2')
      expect(names).not.toContain('mvdeep/l1/l2/l3')
      // New destination chain should exist
      expect(names).toContain('mvdeep/other')
      expect(names).toContain('mvdeep/other/place')
    })

    it('should cleanup source prefixes when moving to root (no destination prefix)', async () => {
      await createObject('rootmv/folder/file.txt')

      // Move to root
      await moveObject('rootmv/folder/file.txt', 'file_at_root.txt')

      // Root files do not create prefixes, so everything should be cleaned up
      const prefixes = await getPrefixes()
      expect(prefixes).toHaveLength(0)
    })

    it('should handle concurrent moves from the same source folder without dangling prefixes', async () => {
      await createObject('race-move/src/f1.txt')
      await createObject('race-move/src/f2.txt')
      await createObject('race-move/src/f3.txt')
      await createObject('race-move/src/f4.txt')

      const moves = [
        moveObject('race-move/src/f1.txt', 'race-move/dst/f1.txt'),
        moveObject('race-move/src/f2.txt', 'race-move/dst/f2.txt'),
        moveObject('race-move/src/f3.txt', 'race-move/dst/f3.txt'),
        moveObject('race-move/src/f4.txt', 'race-move/dst/f4.txt'),
      ]

      await Promise.all(moves)

      const prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'race-move', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'race-move/dst', level: 2 })
      expect(prefixes).not.toContainEqual({
        bucket_id: bucketName,
        name: 'race-move/src',
        level: 2,
      })
    })
  })

  describe('Stress Test: Move Operations', () => {
    it('should handle many concurrent moves and clean old prefixes correctly', async () => {
      const sources = ['mvstress/src1', 'mvstress/src2', 'mvstress/src3']
      const subs = ['sub1', 'sub2', 'sub3']
      const countPerSub = 5

      // Create source objects
      const objects: string[] = []
      for (const s of sources) {
        for (const sub of subs) {
          for (let i = 0; i < countPerSub; i++) {
            const name = `${s}/${sub}/file${i}.txt`
            objects.push(name)
            await createObject(name)
          }
        }
      }

      // Verify initial prefixes exist
      let prefixes = await getPrefixes()
      const namesBefore = prefixes.map((p) => p.name)
      expect(namesBefore).toEqual(
        expect.arrayContaining([
          'mvstress',
          ...sources,
          ...sources.flatMap((s) => subs.map((sub) => `${s}/${sub}`)),
        ])
      )

      // Concurrently move all files into mvstress/dst while preserving sub-structure
      const movePromises: Promise<any>[] = []
      for (const s of sources) {
        for (const sub of subs) {
          for (let i = 0; i < countPerSub; i++) {
            const src = `${s}/${sub}/file${i}.txt`
            const dst = `mvstress/dst/${sub}/file_${s.split('/').pop()}_${i}.txt`
            movePromises.push(moveObject(src, dst))
          }
        }
      }

      await Promise.all(movePromises)

      // After moves: source prefixes should be gone, destination prefixes should exist
      prefixes = await getPrefixes()
      const namesAfter = prefixes.map((p) => p.name)

      // Root mvstress and destination tree
      expect(namesAfter).toContain('mvstress')
      expect(namesAfter).toContain('mvstress/dst')
      for (const sub of subs) {
        expect(namesAfter).toContain(`mvstress/dst/${sub}`)
      }

      // All source roots and their subs should be cleaned up
      for (const s of sources) {
        expect(namesAfter).not.toContain(s)
        for (const sub of subs) {
          expect(namesAfter).not.toContain(`${s}/${sub}`)
        }
      }
    }, 30000)
  })
})
