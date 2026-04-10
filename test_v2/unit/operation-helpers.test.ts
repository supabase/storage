import { describe, expect, it } from 'vitest'
import { getTestKnex } from '@internal/testing/helpers'

// Tests for the storage.allow_only_operation / storage.allow_any_operation
// SQL helpers. These run as postgres superuser against a dedicated per-test
// transaction so we can set storage.operation locally without leaking state.

describe('Storage operation helpers', () => {
  async function selectAllowed(
    sql: string,
    bindings: unknown[] = [],
    currentOperation?: string
  ): Promise<boolean> {
    const db = getTestKnex()
    const trx = await db.transaction()
    try {
      if (currentOperation) {
        await trx.raw(`SELECT set_config('storage.operation', ?, true)`, [currentOperation])
      }
      const result = await trx.raw(sql, bindings)
      return result.rows[0].allowed
    } finally {
      if (!trx.isCompleted()) {
        await trx.rollback()
      }
    }
  }

  it('matches canonical operations through short and full names', async () => {
    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        ['storage.object.list'],
        'storage.object.list'
      )
    ).resolves.toBe(true)

    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        ['object.list'],
        'storage.object.list'
      )
    ).resolves.toBe(true)

    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        ['object.get'],
        'storage.object.list'
      )
    ).resolves.toBe(false)
  })

  it('keeps compatibility with current bare object.* route operation values', async () => {
    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        ['object.get_authenticated_info'],
        'object.get_authenticated_info'
      )
    ).resolves.toBe(true)

    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        ['storage.object.get_authenticated_info'],
        'object.get_authenticated_info'
      )
    ).resolves.toBe(true)
  })

  it('returns false when the current operation is unset or the input is empty', async () => {
    await expect(
      selectAllowed(`SELECT storage.allow_only_operation(?) AS allowed`, ['object.list'])
    ).resolves.toBe(false)

    await expect(
      selectAllowed(
        `SELECT storage.allow_only_operation(?) AS allowed`,
        [''],
        'storage.object.list'
      )
    ).resolves.toBe(false)
  })

  it('matches any provided operation without prefix semantics', async () => {
    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[?, ?]::text[]) AS allowed`,
        ['bucket.get', 'storage.object.list'],
        'storage.object.list'
      )
    ).resolves.toBe(true)

    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[?]::text[]) AS allowed`,
        ['object'],
        'storage.object.list'
      )
    ).resolves.toBe(false)

    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[]::text[]) AS allowed`,
        [],
        'storage.object.list'
      )
    ).resolves.toBe(false)
  })

  it('ignores null and empty entries when matching any operation', async () => {
    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[?, ?, ?]::text[]) AS allowed`,
        [null, '', 'storage.object.list'],
        'storage.object.list'
      )
    ).resolves.toBe(true)

    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[?, ?]::text[]) AS allowed`,
        [null, ''],
        'storage.object.list'
      )
    ).resolves.toBe(false)

    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(NULL::text[]) AS allowed`,
        [],
        'object.list'
      )
    ).resolves.toBe(false)
  })

  it('keeps bare object.* compatibility for allow_any_operation', async () => {
    await expect(
      selectAllowed(
        `SELECT storage.allow_any_operation(ARRAY[?, ?]::text[]) AS allowed`,
        ['bucket.get', 'storage.object.get_authenticated_info'],
        'object.get_authenticated_info'
      )
    ).resolves.toBe(true)
  })
})
