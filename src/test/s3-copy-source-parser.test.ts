'use strict'

import { ErrorCode, StorageBackendError } from '@internal/errors'
import { parseCopySource } from '../storage/protocols/s3/copy-source-parser'

describe('parseCopySource', () => {
  test('preserves raw question marks and hashes in partially encoded keys', () => {
    const objectKey = 'folder/일이삼?x=1#frag.png'
    const result = parseCopySource(`bucket/${encodeURI(objectKey)}`)

    expect(result).toEqual({
      bucketName: 'bucket',
      objectKey,
      sourceVersion: undefined,
    })
  })

  test('preserves question marks in versionId query values', () => {
    const result = parseCopySource(
      'bucket/folder%20one/%EC%9D%BC%EC%9D%B4%EC%82%BC.png?versionId=v1?part=2'
    )

    expect(result).toEqual({
      bucketName: 'bucket',
      objectKey: 'folder one/일이삼.png',
      sourceVersion: 'v1?part=2',
    })
  })

  test('accepts fully URL-encoded CopySource values with versionId', () => {
    const result = parseCopySource(
      `${encodeURIComponent('bucket/folder one/일이삼/🙂?#%.png')}?versionId=ver-123`
    )

    expect(result).toEqual({
      bucketName: 'bucket',
      objectKey: 'folder one/일이삼/🙂?#%.png',
      sourceVersion: 'ver-123',
    })
  })

  test('accepts fully URL-encoded leading-slash CopySource values', () => {
    const result = parseCopySource(encodeURIComponent('/bucket/folder one/일이삼/🙂?#%.png'))

    expect(result).toEqual({
      bucketName: 'bucket',
      objectKey: 'folder one/일이삼/🙂?#%.png',
      sourceVersion: undefined,
    })
  })

  test('rejects an empty versionId query value', () => {
    expect(() => parseCopySource('bucket/key?versionId=')).toThrow(
      expect.objectContaining<Partial<StorageBackendError>>({
        code: ErrorCode.MissingParameter,
        message: 'Invalid Parameter CopySource',
      })
    )
  })
})
