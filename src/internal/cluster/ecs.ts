import { ECSClient, ListTasksCommand } from '@aws-sdk/client-ecs'
import axios from 'axios'
import { DesiredStatus } from '@aws-sdk/client-ecs/dist-types/models/models_0'

export class ClusterDiscoveryECS {
  private client: ECSClient

  constructor() {
    this.client = new ECSClient()
  }

  async getClusterSize() {
    if (!process.env.ECS_CONTAINER_METADATA_URI) {
      throw new Error('ECS_CONTAINER_METADATA_URI is not set')
    }

    const [running, pending] = await Promise.all([
      this.listTasks('RUNNING'),
      this.listTasks('PENDING'),
    ])

    return running + pending
  }

  private async listTasks(status: DesiredStatus) {
    const respMetadata = await axios.get(`${process.env.ECS_CONTAINER_METADATA_URI}/task`)

    const command = new ListTasksCommand({
      family: respMetadata.data.Family,
      cluster: respMetadata.data.Cluster,
      desiredStatus: status,
    })
    const response = await this.client.send(command)
    return response.taskArns?.length || 0
  }
}
