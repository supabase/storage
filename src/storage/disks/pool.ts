// import { StorageDisk } from '@storage/disks/adapter'
// import { StorageBackendType } from '../../config'
//
// export class DiskPool {
//   private disks: Map<string, StorageDisk> = new Map()
//
//   acquire(id: string, type: StorageBackendType) {
//     const disk = this.disks.get(id)
//     if (disk) {
//       return disk
//     }
//     const newDisk = createDisk(type, {
//       mountBucket: id,
//     })
//   }
//
//   getDisks() {
//     return this.disks
//   }
// }
