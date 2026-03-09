'use strict'

import {
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  S3Client,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { ObjectBackup } from '../storage/backend/s3/backup'

jest.mock('@aws-sdk/client-s3', () => {
  const originalModule = jest.requireActual('@aws-sdk/client-s3')
  return {
    ...originalModule,
    S3Client: jest.fn().mockImplementation(() => ({
      send: jest.fn(),
    })),
  }
})

const encodeCopySourceByPathToken = (bucket: string, key: string) =>
  `${encodeURIComponent(bucket)}/${key
    .split('/')
    .map((pathToken) => encodeURIComponent(pathToken))
    .join('/')}`

describe('ObjectBackup', () => {
  let mockSend: jest.Mock
  let client: S3Client

  beforeEach(() => {
    jest.clearAllMocks()
    mockSend = jest.fn()
    ;(S3Client as jest.Mock).mockImplementation(() => ({
      send: mockSend,
    }))
    client = new S3Client({}) as unknown as S3Client
  })

  test('singleCopy preserves path separators for Unicode source keys', async () => {
    mockSend.mockResolvedValue({})

    const sourceKey = 'folder one/일이삼/子目录/🙂?#%.png'
    const destinationKey = 'backup/folder/복사본.png'

    const backup = new ObjectBackup(client, {
      sourceBucket: 'source-bucket',
      sourceKey,
      destinationBucket: 'backup-bucket',
      destinationKey,
      size: 1024,
    })

    await backup.backup()

    expect(mockSend).toHaveBeenCalledTimes(1)
    const command = mockSend.mock.calls[0][0] as CopyObjectCommand
    expect(command).toBeInstanceOf(CopyObjectCommand)
    expect(command.input.CopySource).toBe(encodeCopySourceByPathToken('source-bucket', sourceKey))
    expect(command.input.CopySource).toContain('source-bucket/folder%20one/')
    expect(command.input.CopySource).not.toContain('%2Fsource-bucket%2F')
    expect(command.input.CopySource).not.toContain('source-bucket%2F')
  })

  test('multipartCopy preserves path separators for Unicode source keys', async () => {
    mockSend.mockImplementation((command: unknown) => {
      if (command instanceof CreateMultipartUploadCommand) {
        return Promise.resolve({ UploadId: 'upload-id' })
      }

      if (command instanceof UploadPartCopyCommand) {
        return Promise.resolve({
          CopyPartResult: {
            ETag: `"etag-${command.input.PartNumber}"`,
          },
        })
      }

      if (command instanceof CompleteMultipartUploadCommand) {
        return Promise.resolve({})
      }

      return Promise.resolve({})
    })

    const sourceKey = 'folder one/일이삼/子目录/🙂?#%.png'
    const destinationKey = 'backup/folder/복사본.png'
    const partSize = 5 * 1024 * 1024 * 1024
    const backup = new ObjectBackup(client, {
      sourceBucket: 'source-bucket',
      sourceKey,
      destinationBucket: 'backup-bucket',
      destinationKey,
      size: partSize + 1024,
    })

    await backup.backup()

    const uploadPartCommands = mockSend.mock.calls
      .map(([command]) => command)
      .filter(
        (command): command is UploadPartCopyCommand => command instanceof UploadPartCopyCommand
      )

    expect(uploadPartCommands).toHaveLength(2)
    for (const command of uploadPartCommands) {
      expect(command.input.CopySource).toBe(encodeCopySourceByPathToken('source-bucket', sourceKey))
      expect(command.input.CopySource).toContain('source-bucket/folder%20one/')
      expect(command.input.CopySource).not.toContain('%2Fsource-bucket%2F')
      expect(command.input.CopySource).not.toContain('source-bucket%2F')
    }
  })
})
