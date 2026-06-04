import { PutVectorsCommand, S3VectorsClient, ValidationException } from '@aws-sdk/client-s3vectors'
import { ErrorCode } from '@internal/errors'
import { type Mock, vi } from 'vitest'
import { S3Vector } from './s3-vector'

describe('S3Vector', () => {
  let send: Mock<S3VectorsClient['send']>

  beforeEach(() => {
    send = vi.fn()
  })

  function createStore() {
    return new S3Vector({ send } as unknown as S3VectorsClient)
  }

  it('maps S3Vectors validation failures to invalid parameter errors', async () => {
    const upstreamError = new ValidationException({
      message:
        "Invalid record for key '5797803-0': Filterable metadata must have at most 2048 bytes",
      $metadata: { httpStatusCode: 400 },
      fieldList: [
        {
          path: 'vectors[0].metadata',
          message: 'Filterable metadata must have at most 2048 bytes',
        },
      ],
    })
    send.mockRejectedValueOnce(upstreamError)

    await expect(
      createStore().putVectors({
        vectorBucketName: 'bucket',
        indexName: 'index',
        vectors: [{ key: '5797803-0', data: { float32: [1, 2, 3] } }],
      })
    ).rejects.toMatchObject({
      code: ErrorCode.InvalidParameter,
      httpStatusCode: 400,
      message: upstreamError.message,
      originalError: upstreamError,
    })

    expect(send).toHaveBeenCalledWith(expect.any(PutVectorsCommand))
  })
})
