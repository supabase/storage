import { ListTasksCommand } from '@aws-sdk/client-ecs'
import { vi } from 'vitest'
import { ClusterDiscoveryECS } from './ecs'

const { mockSend } = vi.hoisted(() => ({
  mockSend: vi.fn(),
}))

vi.mock('@aws-sdk/client-ecs', async () => {
  const originalModule =
    await vi.importActual<typeof import('@aws-sdk/client-ecs')>('@aws-sdk/client-ecs')

  return {
    ...originalModule,
    ECSClient: vi.fn(function () {
      return {
        send: mockSend,
      }
    }),
  }
})

describe('ClusterDiscoveryECS', () => {
  const originalMetadataUri = process.env.ECS_CONTAINER_METADATA_URI

  beforeEach(() => {
    vi.clearAllMocks()
    vi.unstubAllGlobals()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()

    if (originalMetadataUri === undefined) {
      delete process.env.ECS_CONTAINER_METADATA_URI
    } else {
      process.env.ECS_CONTAINER_METADATA_URI = originalMetadataUri
    }
  })

  it('throws when ECS task metadata URI is not configured', async () => {
    delete process.env.ECS_CONTAINER_METADATA_URI

    await expect(new ClusterDiscoveryECS().getClusterSize()).rejects.toThrow(
      'ECS_CONTAINER_METADATA_URI is not set'
    )

    expect(mockSend).not.toHaveBeenCalled()
  })

  it('fetches ECS task metadata once and counts running plus pending tasks', async () => {
    process.env.ECS_CONTAINER_METADATA_URI = 'http://169.254.170.2/v4/metadata'

    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        JSON.stringify({
          Cluster: 'cluster-a',
          Family: 'storage',
        }),
        {
          status: 200,
          headers: {
            'content-type': 'application/json',
          },
        }
      )
    )
    vi.stubGlobal('fetch', fetchMock)

    mockSend
      .mockResolvedValueOnce({
        taskArns: ['task-1', 'task-2'],
      })
      .mockResolvedValueOnce({
        taskArns: ['task-3'],
      })

    await expect(new ClusterDiscoveryECS().getClusterSize()).resolves.toBe(3)

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(fetchMock).toHaveBeenCalledWith('http://169.254.170.2/v4/metadata/task')
    expect(mockSend).toHaveBeenCalledTimes(2)
    expect(mockSend.mock.calls[0][0]).toBeInstanceOf(ListTasksCommand)
    expect(mockSend.mock.calls[0][0].input).toEqual({
      cluster: 'cluster-a',
      desiredStatus: 'RUNNING',
      family: 'storage',
    })
    expect(mockSend.mock.calls[1][0]).toBeInstanceOf(ListTasksCommand)
    expect(mockSend.mock.calls[1][0].input).toEqual({
      cluster: 'cluster-a',
      desiredStatus: 'PENDING',
      family: 'storage',
    })
  })

  it('drains failed ECS task metadata responses before listing tasks', async () => {
    process.env.ECS_CONTAINER_METADATA_URI = 'http://169.254.170.2/v4/metadata'

    const cancel = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue({
      ok: false,
      status: 503,
      statusText: 'Service Unavailable',
      body: {
        cancel,
      },
    } as unknown as Response)
    vi.stubGlobal('fetch', fetchMock)

    await expect(new ClusterDiscoveryECS().getClusterSize()).rejects.toThrow(
      'Request failed with status code 503 Service Unavailable fetching ECS task metadata from http://169.254.170.2/v4/metadata/task'
    )

    expect(fetchMock).toHaveBeenCalledWith('http://169.254.170.2/v4/metadata/task')
    expect(cancel).toHaveBeenCalledTimes(1)
    expect(mockSend).not.toHaveBeenCalled()
  })
})
