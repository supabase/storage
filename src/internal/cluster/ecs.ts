import { type DesiredStatus, ECSClient, ListTasksCommand } from '@aws-sdk/client-ecs'

type ECSTaskMetadata = {
  Cluster: string
  Family: string
}

export class ClusterDiscoveryECS {
  private client: ECSClient

  constructor() {
    this.client = new ECSClient()
  }

  async getClusterSize() {
    if (!process.env.ECS_CONTAINER_METADATA_URI) {
      throw new Error('ECS_CONTAINER_METADATA_URI is not set')
    }

    const metadata = await this.getTaskMetadata(process.env.ECS_CONTAINER_METADATA_URI)

    const [running, pending] = await Promise.all([
      this.listTasks(metadata, 'RUNNING'),
      this.listTasks(metadata, 'PENDING'),
    ])

    return running + pending
  }

  private async getTaskMetadata(metadataUri: string): Promise<ECSTaskMetadata> {
    const metadataUrl = `${metadataUri}/task`
    const response = await fetch(metadataUrl)

    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined)
      const statusText = response.statusText ? ` ${response.statusText}` : ''
      throw new Error(
        `Request failed with status code ${response.status}${statusText} fetching ECS task metadata from ${metadataUrl}`
      )
    }

    return (await response.json()) as ECSTaskMetadata
  }

  private async listTasks(metadata: ECSTaskMetadata, status: DesiredStatus) {
    const command = new ListTasksCommand({
      family: metadata.Family,
      cluster: metadata.Cluster,
      desiredStatus: status,
    })
    const response = await this.client.send(command)
    return response.taskArns?.length || 0
  }
}
