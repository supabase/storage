import { ECSClient, ListTasksCommand } from '@aws-sdk/client-ecs'
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

const METADATA_URI = 'http://ecs-metadata.example/v4/metadata'
const TASK_METADATA_URL = `${METADATA_URI}/task`
const LIST_TASKS_INPUT = { cluster: 'cluster-a', family: 'storage', desiredStatus: 'RUNNING' }
const signal = new AbortController().signal
const EMPTY_TASK_ARNS = [{ taskArns: [] }, { taskArns: undefined }]

function listTasksInputs() {
  return mockSend.mock.calls.map(([command]) => {
    expect(command).toBeInstanceOf(ListTasksCommand)
    return command.input
  })
}

function nextTokens() {
  return listTasksInputs().map(({ nextToken }) => nextToken)
}

describe('ClusterDiscoveryECS', () => {
  beforeEach(() => {
    mockSend.mockReset()
    vi.stubEnv('AWS_MAX_ATTEMPTS', undefined)
    vi.stubEnv('ECS_CONTAINER_METADATA_URI', METADATA_URI)
    vi.stubGlobal(
      'fetch',
      vi.fn<typeof fetch>(async () => Response.json({ Cluster: 'cluster-a', Family: 'storage' }))
    )
  })

  it('throws when ECS task metadata URI is not configured', async () => {
    vi.stubEnv('ECS_CONTAINER_METADATA_URI', undefined)

    await expect(new ClusterDiscoveryECS().getClusterSize(signal)).rejects.toThrow(
      'ECS_CONTAINER_METADATA_URI is not set'
    )

    expect(mockSend).not.toHaveBeenCalled()
  })

  it('configures the ECS client with a retry provider and a request timeout', () => {
    new ClusterDiscoveryECS()

    expect(ECSClient).toHaveBeenLastCalledWith({
      maxAttempts: expect.any(Function),
      requestHandler: { requestTimeout: 10_000, throwOnRequestTimeout: true },
    })
  })

  it('fetches ECS task metadata once and reuses it across cluster size checks', async () => {
    mockSend
      .mockResolvedValueOnce({ taskArns: ['task-1'] })
      .mockResolvedValueOnce({ taskArns: ['task-1', 'task-2'] })

    const clusterDiscovery = new ClusterDiscoveryECS()

    await expect(clusterDiscovery.getClusterSize(signal)).resolves.toBe(1)
    await expect(clusterDiscovery.getClusterSize(signal)).resolves.toBe(2)

    expect(fetch).toHaveBeenCalledTimes(1)
    expect(fetch).toHaveBeenCalledWith(TASK_METADATA_URL, { signal })
    expect(listTasksInputs()).toEqual([LIST_TASKS_INPUT, LIST_TASKS_INPUT])
  })

  it('drains failed ECS task metadata responses before listing tasks', async () => {
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

    await expect(new ClusterDiscoveryECS().getClusterSize(signal)).rejects.toThrow(
      `Request failed with status code 503 Service Unavailable fetching ECS task metadata from ${TASK_METADATA_URL}`
    )

    expect(fetchMock).toHaveBeenCalledWith(TASK_METADATA_URL, { signal })
    expect(cancel).toHaveBeenCalledTimes(1)
    expect(mockSend).not.toHaveBeenCalled()
  })

  it('retries task metadata after a failed fetch', async () => {
    vi.mocked(fetch).mockRejectedValueOnce(new Error('aborted'))
    mockSend.mockResolvedValueOnce({ taskArns: ['task-1'] })
    const discovery = new ClusterDiscoveryECS()

    await expect(discovery.getClusterSize(signal)).rejects.toThrow('aborted')
    await expect(discovery.getClusterSize(signal)).resolves.toBe(1)
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  describe('task pagination', () => {
    it('counts every page and preserves the task filters and continuation tokens', async () => {
      mockSend
        .mockResolvedValueOnce({
          taskArns: Array.from({ length: 100 }, (_, index) => `task-${index}`),
          nextToken: 'page-2',
        })
        .mockResolvedValueOnce({ taskArns: ['task-100'], nextToken: 'page-3' })
        .mockResolvedValueOnce({ taskArns: ['task-101', 'task-102'] })

      await expect(new ClusterDiscoveryECS().getClusterSize(signal)).resolves.toBe(103)

      expect(fetch).toHaveBeenCalledTimes(1)
      expect(listTasksInputs()).toEqual(
        [undefined, 'page-2', 'page-3'].map((nextToken) => ({ ...LIST_TASKS_INPUT, nextToken }))
      )
      expect(mockSend.mock.calls.map(([, options]) => options)).toEqual(
        Array(3).fill({ abortSignal: signal })
      )
    })

    it.each(EMPTY_TASK_ARNS)('continues when an intermediate page has taskArns=$taskArns', async ({
      taskArns,
    }) => {
      mockSend
        .mockResolvedValueOnce({ taskArns: ['task-1'], nextToken: 'page-2' })
        .mockResolvedValueOnce({ taskArns, nextToken: 'page-3' })
        .mockResolvedValueOnce({ taskArns: ['task-2'] })

      await expect(new ClusterDiscoveryECS().getClusterSize(signal)).resolves.toBe(2)
      expect(nextTokens()).toEqual([undefined, 'page-2', 'page-3'])
    })

    it.each(
      EMPTY_TASK_ARNS
    )('returns zero for taskArns=$taskArns without a continuation token', async ({ taskArns }) => {
      mockSend.mockResolvedValueOnce({ taskArns })

      await expect(new ClusterDiscoveryECS().getClusterSize(signal)).resolves.toBe(0)
      expect(mockSend).toHaveBeenCalledTimes(1)
    })

    it('rejects a failed continuation page and starts the next check from the first page', async () => {
      const error = new Error('ECS ListTasks failed')
      mockSend
        .mockResolvedValueOnce({ taskArns: ['task-1'], nextToken: 'page-2' })
        .mockRejectedValueOnce(error)
        .mockResolvedValueOnce({ taskArns: ['task-1', 'task-2'] })
      const discovery = new ClusterDiscoveryECS()

      await expect(discovery.getClusterSize(signal)).rejects.toBe(error)
      await expect(discovery.getClusterSize(signal)).resolves.toBe(2)

      expect(fetch).toHaveBeenCalledTimes(1)
      expect(nextTokens()).toEqual([undefined, 'page-2', undefined])
    })
  })
})
