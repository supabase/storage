import { Sharder, ShardResource } from '../sharder'
import { ResourceKind, ShardRow, ShardStatus } from '@internal/sharding/store'

export class SingleShard implements Sharder {
  constructor(
    protected readonly singleShard: {
      shardKey: string
      capacity: number
    }
  ) {}

  freeByResource(): Promise<void> {
    return Promise.resolve()
  }

  cancel(): Promise<void> {
    return Promise.resolve(undefined)
  }

  confirm(): Promise<void> {
    return Promise.resolve(undefined)
  }

  createShard(opts: {
    kind: ResourceKind
    shardKey: string
    capacity?: number
    status?: ShardStatus
  }): Promise<ShardRow> {
    return Promise.resolve({
      shard_key: opts.shardKey,
      capacity: opts.capacity || this.singleShard.capacity,
      kind: opts.kind,
      id: this.singleShard.shardKey,
      status: 'active',
      next_slot: 1,
      created_at: new Date().toISOString(),
    })
  }

  expireLeases(): Promise<number> {
    return Promise.resolve(0)
  }

  findShardByResourceId(param: ShardResource): Promise<ShardRow> {
    return Promise.resolve({
      id: this.singleShard.shardKey,
      kind: param.kind,
      shard_key: this.singleShard.shardKey,
      capacity: this.singleShard.capacity,
      status: 'active',
      next_slot: 1,
      created_at: new Date().toISOString(),
    })
  }

  freeByLocation(): Promise<void> {
    return Promise.resolve(undefined)
  }

  reserve(): Promise<{
    reservationId: string
    shardId: string
    shardKey: string
    slotNo: number
    leaseExpiresAt: string
  }> {
    return Promise.resolve({
      leaseExpiresAt: '',
      reservationId: '',
      shardId: this.singleShard.shardKey,
      shardKey: this.singleShard.shardKey,
      slotNo: 0,
    })
  }

  setShardStatus(): Promise<void> {
    return Promise.resolve(undefined)
  }

  shardStats(): Promise<any> {
    return Promise.resolve(undefined)
  }
}
