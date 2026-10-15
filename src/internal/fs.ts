import * as fs from 'node:fs/promises'
import path from 'node:path'

export async function ensureDir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true })
}

export async function ensureFile(filePath: string, attempts = 5): Promise<void> {
  try {
    await ensureDir(path.dirname(filePath))

    // Open in append mode so missing files are created without truncating existing ones.
    const handle = await fs.open(filePath, 'a')
    await handle.close()
  } catch (e) {
    // Empty-directory cleanup after a concurrent delete can remove a parent before the file exists.
    if ((e as NodeJS.ErrnoException).code !== 'ENOENT' || attempts <= 1) throw e
    await ensureFile(filePath, attempts - 1)
  }
}

export async function pathExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath)
    return true
  } catch {
    return false
  }
}

export async function removePath(filePath: string): Promise<void> {
  await fs.rm(filePath, { recursive: true, force: true })
}
