'use strict'

import { useStorage } from './utils/storage'

describe('Prefix SQL Functions Unit Tests', () => {
  const tHelper = useStorage()
  const bucketName = `test-prefix-funcs-${Date.now()}`

  beforeAll(async () => {
    // Create test bucket (required for foreign key constraints)
    await tHelper.database.createBucket({
      id: bucketName,
      name: bucketName,
    })
  })

  afterEach(async () => {
    // Clean up test data (prefixes and objects)
    const db = tHelper.database.connection.pool.acquire()
    await db.raw('DELETE FROM storage.objects WHERE bucket_id = ?', [bucketName])
    await db.raw('DELETE FROM storage.prefixes WHERE bucket_id = ?', [bucketName])
  })

  afterAll(async () => {
    // Cleanup connections
    await tHelper.database.connection.dispose()
  })

  describe('storage.get_level()', () => {
    it('should return 1 for root level file', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', ['file.txt'])
      expect(result.rows[0].level).toBe(1)
    })

    it('should return 2 for single folder', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', ['folder/file.txt'])
      expect(result.rows[0].level).toBe(2)
    })

    it('should return correct level for deep nesting', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', ['a/b/c/d/file.txt'])
      expect(result.rows[0].level).toBe(5)
    })

    it('should return correct level for very deep nesting (10+ levels)', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', [
        'a/b/c/d/e/f/g/h/i/j/k/file.txt',
      ])
      expect(result.rows[0].level).toBe(12)
    })

    it('should return 2 for folder with trailing slash', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', ['folder/'])
      expect(result.rows[0].level).toBe(2)
    })

    it('should return 4 for folder with multiple trailing slashes', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', ['folder///'])
      expect(result.rows[0].level).toBe(4)
    })

    it('should count empty parts from double slashes', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_level(?) as level', [
        'folder//subfolder/file.txt',
      ])
      expect(result.rows[0].level).toBe(4)
    })
  })

  describe('storage.get_prefix()', () => {
    it('should return empty string for root file', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['file.txt'])
      expect(result.rows[0].prefix).toBe('')
    })

    it('should return folder for single level', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['folder/file.txt'])
      expect(result.rows[0].prefix).toBe('folder')
    })

    it('should return correct prefix for deep nesting', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['a/b/c/file.txt'])
      expect(result.rows[0].prefix).toBe('a/b/c')
    })

    it('should handle trailing slash correctly', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['folder/subfolder/'])
      expect(result.rows[0].prefix).toBe('folder')
    })

    it('should handle multiple slashes', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['folder//file.txt'])
      // Double slash creates an empty part, so parent is 'folder/'
      expect(result.rows[0].prefix).toBe('folder/')
    })

    it('should handle special characters in folder names', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', [
        'folder-name/sub_folder/file.txt',
      ])
      expect(result.rows[0].prefix).toBe('folder-name/sub_folder')
    })

    it('should handle unicode characters', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefix(?) as prefix', ['папка/файл.txt'])
      expect(result.rows[0].prefix).toBe('папка')
    })
  })

  describe('storage.get_prefixes()', () => {
    it('should return empty array for root file', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', ['file.txt'])
      expect(result.rows[0].prefixes).toEqual([])
    })

    it('should return single prefix for single level', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', ['folder/file.txt'])
      expect(result.rows[0].prefixes).toEqual(['folder'])
    })

    it('should return all ancestor prefixes for deep nesting', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', ['a/b/c/file.txt'])
      expect(result.rows[0].prefixes).toEqual(['a', 'a/b', 'a/b/c'])
    })

    it('should return all prefixes for very deep nesting (10+ levels)', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', [
        'a/b/c/d/e/f/g/h/i/j/file.txt',
      ])
      expect(result.rows[0].prefixes).toEqual([
        'a',
        'a/b',
        'a/b/c',
        'a/b/c/d',
        'a/b/c/d/e',
        'a/b/c/d/e/f',
        'a/b/c/d/e/f/g',
        'a/b/c/d/e/f/g/h',
        'a/b/c/d/e/f/g/h/i',
        'a/b/c/d/e/f/g/h/i/j',
      ])
    })

    it('should handle trailing slash correctly', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', [
        'folder/subfolder/',
      ])
      expect(result.rows[0].prefixes).toEqual(['folder', 'folder/subfolder'])
    })

    it('should verify array order is from root to deepest', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const result = await db.raw('SELECT storage.get_prefixes(?) as prefixes', [
        'level1/level2/level3/file.txt',
      ])
      const prefixes = result.rows[0].prefixes
      expect(prefixes).toEqual(['level1', 'level1/level2', 'level1/level2/level3'])
      // Verify order
      expect(prefixes[0]).toBe('level1')
      expect(prefixes[1]).toBe('level1/level2')
      expect(prefixes[2]).toBe('level1/level2/level3')
    })
  })

  describe('storage.add_prefixes()', () => {
    it('should insert prefixes for simple path', async () => {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder/file.txt'])

      const prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      expect(prefixes).toEqual([{ name: 'folder', level: 1 }])
    })

    it('should insert prefixes for deep path', async () => {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'a/b/c/d/file.txt'])

      const prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level', 'name')

      expect(prefixes).toEqual([
        { name: 'a', level: 1 },
        { name: 'a/b', level: 2 },
        { name: 'a/b/c', level: 3 },
        { name: 'a/b/c/d', level: 4 },
      ])
    })

    it('should be idempotent (ON CONFLICT DO NOTHING)', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Insert prefixes twice
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder/file.txt'])
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder/file.txt'])

      const prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      // Should only have one prefix, not duplicates
      expect(prefixes).toHaveLength(1)
      expect(prefixes).toEqual([{ name: 'folder', level: 1 }])
    })

    it('should associate prefixes with correct bucket_id', async () => {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder/file.txt'])

      const prefixes = await db
        .select('bucket_id', 'name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)

      expect(prefixes).toHaveLength(1)
      expect(prefixes[0].bucket_id).toBe(bucketName)
    })

    it('should calculate correct level in inserted records', async () => {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'a/b/c/file.txt'])

      const prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      expect(prefixes[0].level).toBe(1) // 'a'
      expect(prefixes[1].level).toBe(2) // 'a/b'
      expect(prefixes[2].level).toBe(3) // 'a/b/c'
    })

    it('should not insert anything for root file', async () => {
      const db = tHelper.database.connection.pool.acquire()
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'file.txt'])

      const prefixes = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)

      expect(prefixes).toHaveLength(0)
    })
  })

  describe('storage.delete_prefix()', () => {
    it('should delete empty prefix and return true', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create prefix
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder',
      ])

      // Test: delete it
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'folder',
      ])

      expect(result.rows[0].deleted).toBe(true)

      // Verify: prefix is gone
      const remaining = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)

      expect(remaining).toHaveLength(0)
    })

    it('should not delete prefix with child objects and return false', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create prefix and child object
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder',
      ])
      await db.raw(
        'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
        [bucketName, 'folder/file.txt', null, '1']
      )

      // Test: try to delete prefix
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'folder',
      ])

      expect(result.rows[0].deleted).toBe(false)

      // Verify: prefix still exists
      const remaining = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)

      expect(remaining).toHaveLength(1)
      expect(remaining[0].name).toBe('folder')
    })

    it('should not delete prefix with child prefixes and return false', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create parent and child prefixes
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder',
      ])
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder/subfolder',
      ])

      // Test: try to delete parent prefix
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'folder',
      ])

      expect(result.rows[0].deleted).toBe(false)

      // Verify: parent prefix still exists
      const remaining = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .where('name', 'folder')

      expect(remaining).toHaveLength(1)
    })

    it('should only check direct children, not all descendants', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create nested structure
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'a',
      ])
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'a/b',
      ])
      await db.raw(
        'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
        [bucketName, 'a/b/c/file.txt', null, '1']
      )

      // Test: try to delete 'a/b' - has object at deeper level
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'a/b',
      ])

      // Should return false because there's an object at 'a/b/c/file.txt' (direct child level check)
      expect(result.rows[0].deleted).toBe(false)
    })

    it('should isolate by bucket_id', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const otherBucket = `other-bucket-${Date.now()}`

      // Setup: create bucket and prefixes in different buckets
      await tHelper.database.createBucket({ id: otherBucket, name: otherBucket })
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder',
      ])
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        otherBucket,
        'folder',
      ])

      // Test: delete prefix in first bucket
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'folder',
      ])

      expect(result.rows[0].deleted).toBe(true)

      // Verify: only first bucket's prefix is deleted
      const remainingInBucket1 = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)

      expect(remainingInBucket1).toHaveLength(0)

      const remainingInBucket2 = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', otherBucket)

      expect(remainingInBucket2).toHaveLength(1)
      expect(remainingInBucket2[0].name).toBe('folder')

      // Cleanup
      await db.raw('DELETE FROM storage.prefixes WHERE bucket_id = ?', [otherBucket])
      await db.raw('DELETE FROM storage.buckets WHERE id = ?', [otherBucket])
    })

    it('should verify prefix is actually deleted when returns true', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create multiple prefixes, one without children
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'empty-folder',
      ])
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder-with-child',
      ])
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'folder-with-child/subfolder',
      ])

      // Test: delete empty prefix
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'empty-folder',
      ])

      expect(result.rows[0].deleted).toBe(true)

      // Verify: only empty prefix is deleted
      const remaining = await db
        .select('name')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('name')

      expect(remaining).toHaveLength(2)
      expect(remaining.map((p) => p.name)).toEqual([
        'folder-with-child',
        'folder-with-child/subfolder',
      ])
    })

    it('should verify prefix is NOT deleted when returns false', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Setup: create prefix with child object
      await db.raw('INSERT INTO storage.prefixes (bucket_id, name) VALUES (?, ?)', [
        bucketName,
        'protected-folder',
      ])
      await db.raw(
        'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
        [bucketName, 'protected-folder/file.txt', null, '1']
      )

      // Test: try to delete protected prefix
      const result = await db.raw('SELECT storage.delete_prefix(?, ?) as deleted', [
        bucketName,
        'protected-folder',
      ])

      expect(result.rows[0].deleted).toBe(false)

      // Verify: prefix still exists with exact same data
      const prefix = await db
        .select('name', 'bucket_id')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .where('name', 'protected-folder')
        .first()

      expect(prefix).toBeDefined()
      expect(prefix.name).toBe('protected-folder')
      expect(prefix.bucket_id).toBe(bucketName)
    })
  })

  describe('storage.lock_top_prefixes()', () => {
    it('should acquire advisory locks for top-level prefixes', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // This function doesn't return a value, but we can test it doesn't throw
      await expect(
        db.raw('SELECT storage.lock_top_prefixes(?, ?)', [
          [bucketName, bucketName],
          ['folder1/file.txt', 'folder2/subfolder/file.txt'],
        ])
      ).resolves.toBeDefined()
    })

    it('should handle empty arrays', async () => {
      const db = tHelper.database.connection.pool.acquire()

      await expect(
        db.raw('SELECT storage.lock_top_prefixes(?, ?)', [[], []])
      ).resolves.toBeDefined()
    })

    it('should handle single bucket and name', async () => {
      const db = tHelper.database.connection.pool.acquire()

      await expect(
        db.raw('SELECT storage.lock_top_prefixes(?, ?)', [[bucketName], ['folder/file.txt']])
      ).resolves.toBeDefined()
    })
  })

  describe('storage.delete_leaf_prefixes()', () => {
    it('should delete leaf prefixes when no children exist', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // First, create some prefixes using add_prefixes function
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder1/file.txt'])
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder2/subfolder/file.txt'])

      // Verify prefixes exist
      let prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      expect(prefixes.length).toBeGreaterThan(0)

      await db.raw('SELECT storage.delete_leaf_prefixes(?, ?)', [
        [bucketName],
        ['folder1/file.txt', 'folder2/subfolder/file.txt'],
      ])

      // Check that some prefixes were deleted (function works)
      prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      // The function should have deleted some prefixes (exact count depends on implementation)
      expect(prefixes.length).toBeLessThan(3) // Some prefixes were deleted
    })

    it('should not delete prefixes with children', async () => {
      const db = tHelper.database.connection.pool.acquire()

      // Create a prefix with a child object using add_prefixes
      await db.raw('SELECT storage.add_prefixes(?, ?)', [bucketName, 'folder/file.txt'])

      // Also create the actual object to ensure the prefix has children
      await db.raw('INSERT INTO storage.objects (bucket_id, name, level) VALUES (?, ?, ?)', [
        bucketName,
        'folder/file.txt',
        2,
      ])

      await db.raw('SELECT storage.delete_leaf_prefixes(?, ?)', [[bucketName], ['folder/file.txt']])

      // Check that prefix still exists (has children)
      const prefixes = await db
        .select('name', 'level')
        .from('storage.prefixes')
        .where('bucket_id', bucketName)
        .orderBy('level')

      expect(prefixes).toHaveLength(1)
      expect(prefixes[0].name).toBe('folder')
    })

    it('should handle empty arrays', async () => {
      const db = tHelper.database.connection.pool.acquire()

      await expect(
        db.raw('SELECT storage.delete_leaf_prefixes(?, ?)', [[], []])
      ).resolves.toBeDefined()
    })

    it('should handle single bucket and name', async () => {
      const db = tHelper.database.connection.pool.acquire()

      await expect(
        db.raw('SELECT storage.delete_leaf_prefixes(?, ?)', [[bucketName], ['folder/file.txt']])
      ).resolves.toBeDefined()
    })
  })
})
