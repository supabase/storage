import { ResourceKind, ShardRow, ShardStatus } from './store'

export interface ShardResource {
  kind: ResourceKind
  tenantId: string
  bucketName: string
  logicalName: string
}

export interface Sharder {
  createShard(opts: {
    kind: ResourceKind
    shardKey: string
    capacity?: number
    status?: ShardStatus
  }): Promise<ShardRow>

  setShardStatus(shardId: string | number, status: ShardStatus): Promise<void>

  reserve(
    opts: ShardResource & {
      kind: ResourceKind
      tenantId: string
      bucketName: string
      logicalName: string
    }
  ): Promise<{
    reservationId: string
    shardId: string
    shardKey: string
    slotNo: number
    leaseExpiresAt: string
  }>
  confirm(reservationId: string, resource: ShardResource): Promise<void>
  cancel(reservationId: string): Promise<void>
  expireLeases(): Promise<number>
  freeByLocation(shardId: string | number, slotNo: number): Promise<void>
  freeByResource(shardId: string | number, resource: ShardResource): Promise<void>
  shardStats(kind?: ResourceKind): Promise<any>
  findShardByResourceId(param: ShardResource): Promise<ShardRow | null>
  listShardByKind(icebergTables: ResourceKind): Promise<ShardRow[]>

  withTnx(tnx: unknown): Sharder
}
