import { FileStore as TusFileStore } from '@tus/file-store'
import { Upload } from '@tus/server'
import fsExtra from 'fs-extra'
import path from 'path'
import { Configstore } from '@tus/file-store'
import { FileSystemDisk } from '../../disks'

type FileStoreOptions = {
  directory: string
  configstore?: Configstore
  expirationPeriodInMilliseconds?: number
}

export class FileStore extends TusFileStore {
  protected fileAdapter: FileSystemDisk

  constructor(protected readonly options: FileStoreOptions) {
    super(options)
    this.fileAdapter = new FileSystemDisk()
  }

  async create(file: Upload): Promise<Upload> {
    const filePath = path.join(this.options.directory, file.id)
    await fsExtra.ensureFile(filePath)

    await this.fileAdapter.setFileMetadata(filePath, {
      cacheControl: file.metadata?.cacheControl || '',
      contentType: file.metadata?.contentType || '',
    })
    await this.configstore.set(file.id, file)
    return file
  }
}
