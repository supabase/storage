import { ECSClient, ListTasksCommand } from '@aws-sdk/client-ecs'
import { loadConfig } from '@smithy/core/config'
import { NODE_MAX_ATTEMPT_CONFIG_OPTIONS } from '@smithy/core/retry'

type ECSTaskMetadata = {
  Cluster: string
  Family: string
}

export class ClusterDiscoveryECS {
  private client: ECSClient
  private taskMetadata?: Promise<ECSTaskMetadata>

  constructor() {
    this.client = new ECSClient({
      maxAttempts: loadConfig({ ...NODE_MAX_ATTEMPT_CONFIG_OPTIONS, default: 10 }),
      requestHandler: { requestTimeout: 10_000, throwOnRequestTimeout: true },
    })
  }

  async getClusterSize(signal: AbortSignal) {
    if (!process.env.ECS_CONTAINER_METADATA_URI) {
      throw new Error('ECS_CONTAINER_METADATA_URI is not set')
    }

    const metadata = await this.getCachedTaskMetadata(
      process.env.ECS_CONTAINER_METADATA_URI,
      signal
    )

    return await this.listTasks(metadata, signal)
  }

  private async getTaskMetadata(
    metadataUri: string,
    signal: AbortSignal
  ): Promise<ECSTaskMetadata> {
    const metadataUrl = `${metadataUri}/task`
    const response = await fetch(metadataUrl, { signal })

    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined)
      const statusText = response.statusText ? ` ${response.statusText}` : ''
      throw new Error(
        `Request failed with status code ${response.status}${statusText} fetching ECS task metadata from ${metadataUrl}`
      )
    }

    return (await response.json()) as ECSTaskMetadata
  }

  private getCachedTaskMetadata(
    metadataUri: string,
    signal: AbortSignal
  ): Promise<ECSTaskMetadata> {
    this.taskMetadata ??= this.getTaskMetadata(metadataUri, signal).catch((error) => {
      this.taskMetadata = undefined
      throw error
    })

    return this.taskMetadata
  }

  private async listTasks(metadata: ECSTaskMetadata, signal: AbortSignal) {
    let taskCount = 0
    let nextToken: string | undefined

    do {
      const command = new ListTasksCommand({
        family: metadata.Family,
        cluster: metadata.Cluster,
        desiredStatus: 'RUNNING',
        nextToken,
      })
      const response = await this.client.send(command, { abortSignal: signal })
      taskCount += response.taskArns?.length ?? 0
      nextToken = response.nextToken
    } while (nextToken)

    return taskCount
  }
}
