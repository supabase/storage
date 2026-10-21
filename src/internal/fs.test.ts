import * as fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { vi } from 'vitest'
import { ensureFile } from './fs'

vi.mock('node:fs/promises', async (importOriginal) => ({
  ...(await importOriginal<typeof fs>()),
}))

describe('ensureFile', () => {
  let tmpDir: string

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'storage-ensure-file-'))
  })

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true })
  })

  it.each([
    'ENOENT',
    'ENOTDIR',
    'EINVAL',
  ])('retries a transient %s from recursive mkdir', async (code) => {
    const file = path.join(tmpDir, 'parent', 'file')
    const error = Object.assign(new Error('Parent disappeared during recursive mkdir'), { code })
    const mkdir = vi.spyOn(fs, 'mkdir').mockRejectedValueOnce(error)

    await ensureFile(file)

    expect(mkdir).toHaveBeenCalledTimes(2)
    await expect(fs.readFile(file, 'utf8')).resolves.toBe('')
  })

  it.each(['ENOENT', 'ENOTDIR'])('stops after five attempts on persistent %s', async (code) => {
    const error = Object.assign(new Error('Cannot create parent'), { code })
    const mkdir = vi.spyOn(fs, 'mkdir').mockRejectedValue(error)

    await expect(ensureFile(path.join(tmpDir, 'parent', 'file'))).rejects.toBe(error)

    expect(mkdir).toHaveBeenCalledTimes(5)
  })

  it('propagates permission errors without retrying', async () => {
    const error = Object.assign(new Error('Permission denied'), { code: 'EACCES' })
    const mkdir = vi.spyOn(fs, 'mkdir').mockRejectedValue(error)

    await expect(ensureFile(path.join(tmpDir, 'parent', 'file'))).rejects.toBe(error)

    expect(mkdir).toHaveBeenCalledTimes(1)
  })

  it('preserves a file that conflicts with a parent directory', async () => {
    const parent = path.join(tmpDir, 'parent')
    await fs.writeFile(parent, 'keep')

    await expect(ensureFile(path.join(parent, 'child', 'file'))).rejects.toMatchObject({
      code: 'ENOTDIR',
    })

    await expect(fs.readFile(parent, 'utf8')).resolves.toBe('keep')
  })

  it('does not truncate an existing file', async () => {
    const file = path.join(tmpDir, 'file')
    await fs.writeFile(file, 'keep')

    await ensureFile(file)

    await expect(fs.readFile(file, 'utf8')).resolves.toBe('keep')
  })
})
