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

  describe('Critical Race Condition: DELETE UPDATE Gap', () => {
    it('should reproduce the race condition between UPDATE and DELETE in statement trigger', async () => {
      await createObject('race/shared/file1.txt')
      await createObject('race/shared/file2.txt')

      const db = tHelper.database.connection.pool.acquire()

      // Simulate the race condition by using two concurrent transactions:
      // Execute concurrent operations that target the same prefix
      await Promise.all([
        // Operation 1: Delete all objects in the prefix (this will try to delete the prefix)
        deleteObjects(['race/shared/file1.txt', 'race/shared/file2.txt']),

        // Operation 2: Immediately create a new object in the same prefix
        // This should increment the counters between the UPDATE and DELETE
        (async () => {
          // Small delay to increase chance of hitting the race window
          await new Promise((resolve) => setTimeout(resolve, 1))
          await createObject('race/shared/file3.txt')
        })(),
      ])

      const objects = await db
        .select('name')
        .from('storage.objects')
        .where('bucket_id', bucketName)
        .where('name', 'like', 'race/shared/%')
      expect(objects.length).toBe(1)

      // If the race condition occurred, the prefix will be incorrectly deleted
      const prefixes = await getPrefixes()
      expect(prefixes.some((p) => p.name === 'race/shared')).toBe(true)
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
      const deletePromises: Promise<object[]>[] = []

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

    it('should handle deadlock scenario in concurrent cross-prefix moves without hanging', async () => {
      // This test reproduces the deadlock scenario where two transactions
      // try to move files between overlapping top-level prefixes in opposite directions:
      // Transaction 1: photos/* -> docs/*  (locks photos -> docs)
      // Transaction 2: docs/* -> photos/*  (locks docs -> photos)

      const setupPromises = [
        createObject('photos/batch1/image1.jpg'),
        createObject('photos/batch1/image2.jpg'),
        createObject('photos/batch2/image3.jpg'),
        createObject('photos/batch2/image4.jpg'),
        createObject('docs/folder1/document1.pdf'),
        createObject('docs/folder1/document2.pdf'),
        createObject('docs/folder2/document3.pdf'),
        createObject('docs/folder2/document4.pdf'),
      ]
      await Promise.all(setupPromises)

      // Verify initial state
      let prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'photos', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'docs', level: 1 })

      // Execute many concurrent moves in both directions to maximize deadlock probability
      await Promise.all([
        // Photos -> Docs moves (locks photos first, then docs)
        moveObject('photos/batch1/image1.jpg', 'docs/moved/image1.jpg'),
        moveObject('photos/batch1/image2.jpg', 'docs/moved/image2.jpg'),
        moveObject('photos/batch2/image3.jpg', 'docs/moved/image3.jpg'),
        moveObject('photos/batch2/image4.jpg', 'docs/moved/image4.jpg'),

        // Docs -> Photos moves (locks docs first, then photos)
        moveObject('docs/folder1/document1.pdf', 'photos/moved/document1.pdf'),
        moveObject('docs/folder1/document2.pdf', 'photos/moved/document2.pdf'),
        moveObject('docs/folder2/document3.pdf', 'photos/moved/document3.pdf'),
        moveObject('docs/folder2/document4.pdf', 'photos/moved/document4.pdf'),
      ])

      // Verify final state: both prefixes should still exist since they have objects
      prefixes = await getPrefixes()
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'photos', level: 1 })
      expect(prefixes).toContainEqual({ bucket_id: bucketName, name: 'docs', level: 1 })

      // Verify all objects were moved correctly
      const db = tHelper.database.connection.pool.acquire()
      const objects = await db
        .select('name')
        .from('storage.objects')
        .where('bucket_id', bucketName)
        .orderBy('name')

      const objectNames = objects.map((o) => o.name)

      // Should have moved images
      expect(objectNames).toContain('docs/moved/image1.jpg')
      expect(objectNames).toContain('docs/moved/image2.jpg')
      expect(objectNames).toContain('docs/moved/image3.jpg')
      expect(objectNames).toContain('docs/moved/image4.jpg')

      // Should have moved documents
      expect(objectNames).toContain('photos/moved/document1.pdf')
      expect(objectNames).toContain('photos/moved/document2.pdf')
      expect(objectNames).toContain('photos/moved/document3.pdf')
      expect(objectNames).toContain('photos/moved/document4.pdf')

      // Original files should be gone
      expect(objectNames).not.toContain('photos/batch1/image1.jpg')
      expect(objectNames).not.toContain('docs/folder1/document1.pdf')

      // Cleanup should have removed empty intermediate prefixes
      const prefixNames = prefixes.map((p) => p.name)
      expect(prefixNames).not.toContain('photos/batch1')
      expect(prefixNames).not.toContain('photos/batch2')
      expect(prefixNames).not.toContain('docs/folder1')
      expect(prefixNames).not.toContain('docs/folder2')
    })

    it('should handle deadlock scenario with direct database updates (more reliable repro)', async () => {
      // This test uses direct database operations to more reliably reproduce
      // the deadlock scenario, bypassing API limitations

      // Setup: Create test objects
      await createObject('photos/file1.jpg')
      await createObject('docs/file2.pdf')

      // Get database connection
      const db = tHelper.database.connection.pool.acquire()

      // Execute concurrent UPDATE operations directly on the database
      // This more closely matches the bash script scenario
      const updatePromises = [
        // Transaction 1: photos -> docs
        db.raw(
          `
          UPDATE storage.objects
          SET name = 'docs/moved-file1.jpg'
          WHERE bucket_id = ? AND name = 'photos/file1.jpg'
        `,
          [bucketName]
        ),

        // Transaction 2: docs -> photos
        db.raw(
          `
          UPDATE storage.objects
          SET name = 'photos/moved-file2.pdf'
          WHERE bucket_id = ? AND name = 'docs/file2.pdf'
        `,
          [bucketName]
        ),
      ]

      const startTime = Date.now()
      await Promise.all(updatePromises)
      const endTime = Date.now()

      // Should complete without deadlock
      expect(endTime - startTime).toBeLessThan(5000)

      // Verify updates succeeded
      const objects = await db
        .select('name')
        .from('storage.objects')
        .where('bucket_id', bucketName)
        .orderBy('name')

      const objectNames = objects.map((o) => o.name)
      expect(objectNames).toContain('docs/moved-file1.jpg')
      expect(objectNames).toContain('photos/moved-file2.pdf')
      expect(objectNames).not.toContain('photos/file1.jpg')
      expect(objectNames).not.toContain('docs/file2.pdf')
    }, 10000)
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
      const movePromises: Promise<void>[] = []
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

  describe('Very Nested Structure Parent Folder Retention', () => {
    it('should retain parent folders when nested files exist at different levels', async () => {
      // Create a very deeply nested structure with files at multiple levels
      await createObject(
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/users/file1.txt'
      )
      await createObject(
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/auth/file2.txt'
      )
      await createObject(
        'company/departments/engineering/teams/frontend/projects/dashboard/components/file3.txt'
      )
      await createObject('company/departments/marketing/campaigns/2024/q1/file4.txt')
      await createObject('company/departments/marketing/campaigns/2024/q2/file5.txt')

      // Verify all prefixes were created
      let prefixes = await getPrefixes()
      const initialPrefixNames = prefixes.map((p) => p.name).sort()

      const expectedPrefixes = [
        'company',
        'company/departments',
        'company/departments/engineering',
        'company/departments/engineering/teams',
        'company/departments/engineering/teams/backend',
        'company/departments/engineering/teams/backend/projects',
        'company/departments/engineering/teams/backend/projects/api',
        'company/departments/engineering/teams/backend/projects/api/v1',
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints',
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/users',
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/auth',
        'company/departments/engineering/teams/frontend',
        'company/departments/engineering/teams/frontend/projects',
        'company/departments/engineering/teams/frontend/projects/dashboard',
        'company/departments/engineering/teams/frontend/projects/dashboard/components',
        'company/departments/marketing',
        'company/departments/marketing/campaigns',
        'company/departments/marketing/campaigns/2024',
        'company/departments/marketing/campaigns/2024/q1',
        'company/departments/marketing/campaigns/2024/q2',
      ]

      for (const expected of expectedPrefixes) {
        expect(initialPrefixNames).toContain(expected)
      }

      // Delete only the auth endpoint file - should only cleanup auth folder, retain all parent folders
      await deleteObjects([
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/auth/file2.txt',
      ])

      prefixes = await getPrefixes()
      const afterAuthDeleteNames = prefixes.map((p) => p.name)

      // All parent folders should still exist because other content remains
      expect(afterAuthDeleteNames).toContain('company')
      expect(afterAuthDeleteNames).toContain('company/departments')
      expect(afterAuthDeleteNames).toContain('company/departments/engineering')
      expect(afterAuthDeleteNames).toContain('company/departments/engineering/teams')
      expect(afterAuthDeleteNames).toContain('company/departments/engineering/teams/backend')
      expect(afterAuthDeleteNames).toContain(
        'company/departments/engineering/teams/backend/projects'
      )
      expect(afterAuthDeleteNames).toContain(
        'company/departments/engineering/teams/backend/projects/api'
      )
      expect(afterAuthDeleteNames).toContain(
        'company/departments/engineering/teams/backend/projects/api/v1'
      )
      expect(afterAuthDeleteNames).toContain(
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints'
      )
      expect(afterAuthDeleteNames).toContain(
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/users'
      )

      // Only the auth folder should be gone
      expect(afterAuthDeleteNames).not.toContain(
        'company/departments/engineering/teams/backend/projects/api/v1/endpoints/auth'
      )

      // Marketing structure should be completely untouched
      expect(afterAuthDeleteNames).toContain('company/departments/marketing')
      expect(afterAuthDeleteNames).toContain('company/departments/marketing/campaigns')
      expect(afterAuthDeleteNames).toContain('company/departments/marketing/campaigns/2024')
      expect(afterAuthDeleteNames).toContain('company/departments/marketing/campaigns/2024/q1')
      expect(afterAuthDeleteNames).toContain('company/departments/marketing/campaigns/2024/q2')
    })

    it('should cascade delete empty parent folders when all children are removed', async () => {
      // Create nested structure where removing one branch should cascade cleanup
      await createObject('org/division/team1/project1/src/main.ts')
      await createObject('org/division/team1/project1/tests/unit.test.ts')
      await createObject('org/division/team1/project2/docs/readme.md')
      await createObject('org/division/team2/project3/code/app.js')

      // Delete entire project1 (both src and tests files)
      await deleteObjects([
        'org/division/team1/project1/src/main.ts',
        'org/division/team1/project1/tests/unit.test.ts',
      ])

      let prefixes = await getPrefixes()
      let prefixNames = prefixes.map((p) => p.name)

      // project1 and its children should be gone
      expect(prefixNames).not.toContain('org/division/team1/project1')
      expect(prefixNames).not.toContain('org/division/team1/project1/src')
      expect(prefixNames).not.toContain('org/division/team1/project1/tests')

      // But team1 should remain because project2 still exists
      expect(prefixNames).toContain('org/division/team1')
      expect(prefixNames).toContain('org/division/team1/project2')
      expect(prefixNames).toContain('org/division/team1/project2/docs')

      // All other structures should remain
      expect(prefixNames).toContain('org')
      expect(prefixNames).toContain('org/division')
      expect(prefixNames).toContain('org/division/team2')
      expect(prefixNames).toContain('org/division/team2/project3')
      expect(prefixNames).toContain('org/division/team2/project3/code')

      // Now delete the remaining project2 file
      await deleteObjects(['org/division/team1/project2/docs/readme.md'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // Now team1 and all its children should be gone
      expect(prefixNames).not.toContain('org/division/team1')
      expect(prefixNames).not.toContain('org/division/team1/project2')
      expect(prefixNames).not.toContain('org/division/team1/project2/docs')

      // But org/division should remain because team2 still exists
      expect(prefixNames).toContain('org')
      expect(prefixNames).toContain('org/division')
      expect(prefixNames).toContain('org/division/team2')
    })

    it('should handle extremely deep nesting (10+ levels) correctly', async () => {
      // Create files at different depths in an extremely nested structure
      const veryDeepPath = 'level1/level2/level3/level4/level5/level6/level7/level8/level9/level10'
      const mediumDeepPath = 'level1/level2/level3/level4/level5/alternative'
      const shallowPath = 'level1/level2/shallow'

      await createObject(`${veryDeepPath}/deep_file.txt`)
      await createObject(`${mediumDeepPath}/medium_file.txt`)
      await createObject(`${shallowPath}/shallow_file.txt`)

      // Verify all 13 prefixes were created (10 + 6 + 3 - 3 shared)
      let prefixes = await getPrefixes()
      let prefixNames = prefixes.map((p) => p.name)

      // Verify deep chain exists
      for (let i = 1; i <= 10; i++) {
        const partialPath = Array.from({ length: i }, (_, idx) => `level${idx + 1}`).join('/')
        expect(prefixNames).toContain(partialPath)
      }

      // Verify medium chain exists
      expect(prefixNames).toContain('level1/level2/level3/level4/level5/alternative')

      // Verify shallow chain exists
      expect(prefixNames).toContain('level1/level2/shallow')

      // Delete the very deep file - should only cleanup the unique deep part
      await deleteObjects([`${veryDeepPath}/deep_file.txt`])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // Deep-only prefixes should be gone (level6 through level10)
      expect(prefixNames).not.toContain('level1/level2/level3/level4/level5/level6')
      expect(prefixNames).not.toContain('level1/level2/level3/level4/level5/level6/level7')
      expect(prefixNames).not.toContain('level1/level2/level3/level4/level5/level6/level7/level8')
      expect(prefixNames).not.toContain(
        'level1/level2/level3/level4/level5/level6/level7/level8/level9'
      )
      expect(prefixNames).not.toContain(
        'level1/level2/level3/level4/level5/level6/level7/level8/level9/level10'
      )

      // Shared prefixes should remain (level1 through level5)
      expect(prefixNames).toContain('level1')
      expect(prefixNames).toContain('level1/level2')
      expect(prefixNames).toContain('level1/level2/level3')
      expect(prefixNames).toContain('level1/level2/level3/level4')
      expect(prefixNames).toContain('level1/level2/level3/level4/level5')

      // Alternative and shallow paths should be untouched
      expect(prefixNames).toContain('level1/level2/level3/level4/level5/alternative')
      expect(prefixNames).toContain('level1/level2/shallow')
    })
  })

  describe('Selective Prefix Deletion Integrity', () => {
    it('should delete only the exact prefix targeted, preserving siblings and parents', async () => {
      // Create a complex branching structure
      await createObject('root/branchA/subA1/file1.txt')
      await createObject('root/branchA/subA2/file2.txt')
      await createObject('root/branchA/subA3/file3.txt')
      await createObject('root/branchB/subB1/file4.txt')
      await createObject('root/branchB/subB2/file5.txt')
      await createObject('root/branchC/file6.txt')

      // Verify initial structure
      let prefixes = await getPrefixes()
      let prefixNames = prefixes.map((p) => p.name)

      expect(prefixNames).toContain('root')
      expect(prefixNames).toContain('root/branchA')
      expect(prefixNames).toContain('root/branchA/subA1')
      expect(prefixNames).toContain('root/branchA/subA2')
      expect(prefixNames).toContain('root/branchA/subA3')
      expect(prefixNames).toContain('root/branchB')
      expect(prefixNames).toContain('root/branchB/subB1')
      expect(prefixNames).toContain('root/branchB/subB2')
      expect(prefixNames).toContain('root/branchC')

      // Delete only subA2 - should not affect any other prefixes
      await deleteObjects(['root/branchA/subA2/file2.txt'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // Only subA2 should be gone
      expect(prefixNames).not.toContain('root/branchA/subA2')

      // Everything else should remain intact
      expect(prefixNames).toContain('root')
      expect(prefixNames).toContain('root/branchA')
      expect(prefixNames).toContain('root/branchA/subA1')
      expect(prefixNames).toContain('root/branchA/subA3')
      expect(prefixNames).toContain('root/branchB')
      expect(prefixNames).toContain('root/branchB/subB1')
      expect(prefixNames).toContain('root/branchB/subB2')
      expect(prefixNames).toContain('root/branchC')

      // Delete entire branchB (both files) - should only affect branchB and its children
      await deleteObjects(['root/branchB/subB1/file4.txt', 'root/branchB/subB2/file5.txt'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // All branchB related prefixes should be gone
      expect(prefixNames).not.toContain('root/branchB')
      expect(prefixNames).not.toContain('root/branchB/subB1')
      expect(prefixNames).not.toContain('root/branchB/subB2')

      // Root and other branches should remain
      expect(prefixNames).toContain('root')
      expect(prefixNames).toContain('root/branchA')
      expect(prefixNames).toContain('root/branchA/subA1')
      expect(prefixNames).toContain('root/branchA/subA3')
      expect(prefixNames).toContain('root/branchC')
    })

    it('should handle selective deletion in parallel structure branches without interference', async () => {
      // Create multiple parallel directory structures
      const structures = [
        'workspace/project1/src/components/header.tsx',
        'workspace/project1/src/components/footer.tsx',
        'workspace/project1/src/utils/helper.ts',
        'workspace/project1/tests/unit/header.test.ts',
        'workspace/project2/src/services/api.ts',
        'workspace/project2/src/services/auth.ts',
        'workspace/project2/tests/integration/api.test.ts',
        'workspace/project3/docs/readme.md',
        'workspace/project3/docs/api.md',
        'workspace/shared/config/database.ts',
        'workspace/shared/types/user.ts',
      ]

      for (const path of structures) {
        await createObject(path)
      }

      // Delete all of project1's src files but keep tests
      await deleteObjects([
        'workspace/project1/src/components/header.tsx',
        'workspace/project1/src/components/footer.tsx',
        'workspace/project1/src/utils/helper.ts',
      ])

      let prefixes = await getPrefixes()
      let prefixNames = prefixes.map((p) => p.name)

      // project1 src structure should be cleaned up
      expect(prefixNames).not.toContain('workspace/project1/src')
      expect(prefixNames).not.toContain('workspace/project1/src/components')
      expect(prefixNames).not.toContain('workspace/project1/src/utils')

      // But project1 tests should remain
      expect(prefixNames).toContain('workspace/project1')
      expect(prefixNames).toContain('workspace/project1/tests')
      expect(prefixNames).toContain('workspace/project1/tests/unit')

      // All other projects should be completely untouched
      expect(prefixNames).toContain('workspace/project2')
      expect(prefixNames).toContain('workspace/project2/src')
      expect(prefixNames).toContain('workspace/project2/src/services')
      expect(prefixNames).toContain('workspace/project2/tests')
      expect(prefixNames).toContain('workspace/project2/tests/integration')

      expect(prefixNames).toContain('workspace/project3')
      expect(prefixNames).toContain('workspace/project3/docs')

      expect(prefixNames).toContain('workspace/shared')
      expect(prefixNames).toContain('workspace/shared/config')
      expect(prefixNames).toContain('workspace/shared/types')

      // Delete one file from project2 services - should only affect that specific folder
      await deleteObjects(['workspace/project2/src/services/auth.ts'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // services folder should remain (api.ts still exists)
      expect(prefixNames).toContain('workspace/project2/src/services')

      // Now delete the other service file
      await deleteObjects(['workspace/project2/src/services/api.ts'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // Now services folder should be gone, but src should remain because tests exist
      expect(prefixNames).not.toContain('workspace/project2/src/services')
      expect(prefixNames).not.toContain('workspace/project2/src')
      expect(prefixNames).toContain('workspace/project2/tests')
    })

    it('should preserve prefix integrity during concurrent selective deletions', async () => {
      // Create a structure designed to test concurrent deletion integrity
      const files = [
        'concurrent/groupA/item1.txt',
        'concurrent/groupA/item2.txt',
        'concurrent/groupA/item3.txt',
        'concurrent/groupB/item1.txt',
        'concurrent/groupB/item2.txt',
        'concurrent/groupB/item3.txt',
        'concurrent/groupC/item1.txt',
        'concurrent/groupC/item2.txt',
        'concurrent/groupC/item3.txt',
        'concurrent/shared/common1.txt',
        'concurrent/shared/common2.txt',
      ]

      for (const file of files) {
        await createObject(file)
      }

      // Perform concurrent deletions targeting different groups
      const deletePromises = [
        deleteObjects(['concurrent/groupA/item1.txt', 'concurrent/groupA/item2.txt']),
        deleteObjects(['concurrent/groupB/item2.txt']),
        deleteObjects(['concurrent/groupC/item1.txt', 'concurrent/groupC/item3.txt']),
        deleteObjects(['concurrent/shared/common1.txt']),
      ]

      await Promise.all(deletePromises)

      const prefixes = await getPrefixes()
      const prefixNames = prefixes.map((p) => p.name)

      // groupA should remain (item3.txt still exists)
      expect(prefixNames).toContain('concurrent/groupA')

      // groupB should remain (item1.txt and item3.txt still exist)
      expect(prefixNames).toContain('concurrent/groupB')

      // groupC should remain (item2.txt still exists)
      expect(prefixNames).toContain('concurrent/groupC')

      // shared should remain (common2.txt still exists)
      expect(prefixNames).toContain('concurrent/shared')

      // Root concurrent should definitely remain
      expect(prefixNames).toContain('concurrent')

      // Verify no orphaned or extra prefixes exist
      const db = tHelper.database.connection.pool.acquire()
      const allObjects = await db
        .select('name')
        .from('storage.objects')
        .where('bucket_id', bucketName)

      const remainingFiles = allObjects.map((o) => o.name)

      // Verify expected files remain
      expect(remainingFiles).toContain('concurrent/groupA/item3.txt')
      expect(remainingFiles).toContain('concurrent/groupB/item1.txt')
      expect(remainingFiles).toContain('concurrent/groupB/item3.txt')
      expect(remainingFiles).toContain('concurrent/groupC/item2.txt')
      expect(remainingFiles).toContain('concurrent/shared/common2.txt')

      // Verify deleted files are gone
      expect(remainingFiles).not.toContain('concurrent/groupA/item1.txt')
      expect(remainingFiles).not.toContain('concurrent/groupA/item2.txt')
      expect(remainingFiles).not.toContain('concurrent/groupB/item2.txt')
      expect(remainingFiles).not.toContain('concurrent/groupC/item1.txt')
      expect(remainingFiles).not.toContain('concurrent/groupC/item3.txt')
      expect(remainingFiles).not.toContain('concurrent/shared/common1.txt')
    })

    it('should maintain prefix consistency when deleting files with overlapping path names', async () => {
      // Create files with potentially confusing overlapping names
      await createObject('data/user/profile.json')
      await createObject('data/user_settings/theme.json')
      await createObject('data/user_data/cache.json')
      await createObject('data/users/list.json')
      await createObject('metadata/user/info.xml')
      await createObject('metadata/users/directory.xml')

      // Delete user profile - should only affect data/user folder
      await deleteObjects(['data/user/profile.json'])

      let prefixes = await getPrefixes()
      let prefixNames = prefixes.map((p) => p.name)

      // data/user should be gone
      expect(prefixNames).not.toContain('data/user')

      // But similar named folders should remain
      expect(prefixNames).toContain('data/user_settings')
      expect(prefixNames).toContain('data/user_data')
      expect(prefixNames).toContain('data/users')
      expect(prefixNames).toContain('metadata/user')
      expect(prefixNames).toContain('metadata/users')

      // Delete users list - should only affect data/users folder
      await deleteObjects(['data/users/list.json'])

      prefixes = await getPrefixes()
      prefixNames = prefixes.map((p) => p.name)

      // data/users should be gone
      expect(prefixNames).not.toContain('data/users')

      // Other similar folders should still remain
      expect(prefixNames).toContain('data/user_settings')
      expect(prefixNames).toContain('data/user_data')
      expect(prefixNames).toContain('metadata/user')
      expect(prefixNames).toContain('metadata/users')
      expect(prefixNames).toContain('data')
      expect(prefixNames).toContain('metadata')
    })
  })
})
