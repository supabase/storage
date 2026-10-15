import * as fs from 'node:fs/promises'
import path from 'node:path'

export async function ensureDir(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true })
}

const ENSURE_FILE_ATTEMPTS = 5

export async function ensureFile(filePath: string): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await ensureDir(path.dirname(filePath))

      // Open in append mode so missing files are created without truncating existing ones.
      const handle = await fs.open(filePath, 'a')
      await handle.close()
      return
    } catch (e) {
      // Empty-directory cleanup after a concurrent delete can remove a parent before the file exists.
      // Recursive mkdir can also report ENOTDIR if an existing ancestor disappears before its stat.
      const code = (e as NodeJS.ErrnoException).code
      if ((code !== 'ENOENT' && code !== 'ENOTDIR') || attempt >= ENSURE_FILE_ATTEMPTS) throw e
    }
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
