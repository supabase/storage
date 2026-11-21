import fs from 'node:fs/promises'
import path from 'node:path'
import { StreamSplitter } from '@tus/server'

export class TusChunkSplitter extends StreamSplitter {
  override async _newChunk(): Promise<void> {
    const chunkPath = path.join(this.directory, `${this.filenameTemplate}-${this.part}`)
    await this.emitEvent('beforeChunkStarted', chunkPath)

    if (this.destroyed) {
      await this.emitEvent('chunkError', null)
      throw new Error('TUS chunk splitter was destroyed')
    }

    this.currentChunkPath = chunkPath
    const handle = await fs.open(chunkPath, 'w')
    this.fileHandle = handle
    if (!this.destroyed) {
      await this.emitEvent('chunkStarted', chunkPath)
    }
    if (this.destroyed) {
      try {
        await handle.close()
      } finally {
        if (this.fileHandle === handle) this.fileHandle = null
        await fs.rm(chunkPath, { force: true })
      }
      throw new Error('TUS chunk splitter was destroyed')
    }
    this.currentChunkSize = 0
  }

  override async _finishChunk(): Promise<void> {
    if (this.fileHandle === null) return

    await this.fileHandle.close()
    if (this.destroyed) throw new Error('TUS chunk splitter was destroyed')

    await this.emitEvent('chunkFinished', {
      path: this.currentChunkPath,
      size: this.currentChunkSize,
    })
    this.currentChunkPath = null
    this.fileHandle = null
    this.currentChunkSize = 0
    this.part += 1
  }
}
