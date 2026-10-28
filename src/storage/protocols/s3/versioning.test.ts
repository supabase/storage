import { ErrorCode } from '@internal/errors'
import { describe, expect, it } from 'vitest'
import {
  deleteMarkerHeaders,
  parseVersionId,
  toS3VersionId,
  versionIdHeader,
  versionLastModified,
  versionLookup,
} from './versioning'

const VERSION = '0b6f8bb1-5d0f-4fb5-8b1b-0e7c6c2f0a3e'

describe('parseVersionId', () => {
  it('leaves an absent version id unaddressed', () => {
    expect(parseVersionId(undefined)).toBeUndefined()
  })

  it('parses the null version and row version ids', () => {
    expect(parseVersionId('null')).toEqual({ kind: 'null' })
    expect(parseVersionId(VERSION)).toEqual({ kind: 'id', version: VERSION })
  })

  it.each([
    ['', 'Version id cannot be the empty string'],
    ['not-a-version', 'Invalid version id specified'],
    ['NULL', 'Invalid version id specified'],
  ])('rejects %j with S3 message %j', (versionId, message) => {
    expect(() => parseVersionId(versionId)).toThrow(
      expect.objectContaining({ code: ErrorCode.InvalidArgument, httpStatusCode: 400, message })
    )
  })
})

describe('version mapping', () => {
  it('names versioned rows by version and every other row null', () => {
    expect(toS3VersionId({ version: VERSION, is_versioned: true })).toBe(VERSION)
    expect(toS3VersionId({ version: VERSION, is_versioned: false })).toBe('null')
    expect(toS3VersionId({ version: VERSION })).toBe('null')
    // A row written before objects had a version column.
    expect(toS3VersionId({ version: null, is_versioned: false })).toBe('null')
  })

  it('sends a versioned row its version id, whatever the bucket status', () => {
    for (const status of [undefined, 'DISABLED', 'ENABLED', 'SUSPENDED'] as const) {
      expect(versionIdHeader({ version: VERSION, is_versioned: true }, status)).toEqual({
        'x-amz-version-id': VERSION,
      })
    }
  })

  it('sends a null version "null" only once versioning has been configured', () => {
    for (const row of [
      { version: VERSION, is_versioned: false },
      // A row written before objects had a version column.
      { version: null, is_versioned: false },
    ]) {
      expect(versionIdHeader(row, 'ENABLED')).toEqual({ 'x-amz-version-id': 'null' })
      expect(versionIdHeader(row, 'SUSPENDED')).toEqual({ 'x-amz-version-id': 'null' })
      expect(versionIdHeader(row, 'DISABLED')).toEqual({})
      // Writes pass no status.
      expect(versionIdHeader(row)).toEqual({})
    }
    expect(versionIdHeader(undefined, 'ENABLED')).toEqual({})
  })

  it('names the delete marker in the delete marker headers', () => {
    expect(deleteMarkerHeaders({ version: VERSION, is_versioned: true })).toEqual({
      'x-amz-delete-marker': 'true',
      'x-amz-version-id': VERSION,
    })
    // A marker written while versioning was suspended is the null version.
    expect(deleteMarkerHeaders({ version: VERSION, is_versioned: false })).toEqual({
      'x-amz-delete-marker': 'true',
      'x-amz-version-id': 'null',
    })
  })

  it('selects the null version by is_versioned, current or not', () => {
    expect(versionLookup({ kind: 'null' })).toEqual({
      filters: { includeNoncurrent: true, isVersioned: false },
    })
    expect(versionLookup({ kind: 'id', version: VERSION })).toEqual({
      version: VERSION,
      filters: { isVersioned: true },
    })
    expect(versionLookup(undefined)).toEqual({ filters: {} })
  })

  it('reports the upload time whenever it is recorded, current or not', () => {
    const uploadedAt = { lastModified: '2026-10-02T00:00:00.000Z' }
    for (const archived_at of [null, '2026-10-03T00:00:00.000Z']) {
      expect(
        versionLastModified({
          archived_at,
          updated_at: '2026-10-03T00:00:00.000Z',
          created_at: '2026-10-01T00:00:00.000Z',
          metadata: uploadedAt,
        })?.toISOString()
      ).toBe('2026-10-02T00:00:00.000Z')
    }
  })

  it('falls back to updated_at for a current row and created_at for a noncurrent one', () => {
    // Current: updated_at moves on in-place overwrites, created_at does not.
    expect(
      versionLastModified({
        archived_at: null,
        updated_at: '2026-10-03T00:00:00.000Z',
        created_at: '2026-10-01T00:00:00.000Z',
        metadata: null,
      })?.toISOString()
    ).toBe('2026-10-03T00:00:00.000Z')
    // Noncurrent: updated_at is when the row was archived.
    expect(
      versionLastModified({
        archived_at: '2026-10-03T00:00:00.000Z',
        updated_at: '2026-10-03T00:00:00.000Z',
        created_at: '2026-10-01T00:00:00.000Z',
        metadata: {},
      })?.toISOString()
    ).toBe('2026-10-01T00:00:00.000Z')
  })
})
