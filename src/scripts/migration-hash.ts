import { createHash, Hash } from 'crypto'
import fsp from 'fs/promises'
import path from 'path'
import fs from 'fs'

async function computeHash(folder: string, inputHash: Hash | null = null): Promise<string | void> {
  const hash = inputHash ?? createHash('sha256')
  const info = await fsp.readdir(folder, { withFileTypes: true })

  for (const item of info) {
    const fullPath = path.join(folder, item.name)
    if (item.isFile()) {
      const statInfo = await fsp.stat(fullPath)
      // compute hash string name:size:mtime
      const fileInfo = `${fullPath}:${statInfo.size}:${statInfo.mtimeMs}`
      hash.update(fileInfo)
    } else if (item.isDirectory()) {
      // recursively walk sub-folders
      await computeHash(fullPath, hash)
    }
  }

  // if not being called recursively, get the digest and return it as the hash result
  if (!inputHash) {
    return hash.digest('hex')
  }
}

const tenantMigrationFolder = path.join(__dirname, '..', '..', 'migrations', 'tenant')

computeHash(tenantMigrationFolder)
  .then((hash) => {
    if (!hash) {
      throw new Error('Could not compute hash')
    }
    fs.writeFileSync(path.join(__dirname, '..', '..', 'DB_MIGRATION_HASH_FILE'), hash)
  })
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
