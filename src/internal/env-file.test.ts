import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { loadOptionalEnvFile } from './env-file'

test('loads optional env files without overriding existing values', () => {
  const dir = mkdtempSync(path.join(tmpdir(), 'storage-env-'))
  const first = path.join(dir, 'first')
  const second = path.join(dir, 'second')
  writeFileSync(first, 'STORAGE_ENV_TEST_EXISTING=file\nSTORAGE_ENV_TEST_FIRST=first\n')
  writeFileSync(second, 'STORAGE_ENV_TEST_FIRST=second\nSTORAGE_ENV_TEST_LAST="last value"\n')
  vi.stubEnv('STORAGE_ENV_TEST_EXISTING', 'inherited')
  vi.stubEnv('STORAGE_ENV_TEST_FIRST', undefined)
  vi.stubEnv('STORAGE_ENV_TEST_LAST', undefined)

  try {
    loadOptionalEnvFile(path.join(dir, 'missing'))
    loadOptionalEnvFile(first)
    loadOptionalEnvFile(second)
    expect(process.env.STORAGE_ENV_TEST_EXISTING).toBe('inherited')
    expect(process.env.STORAGE_ENV_TEST_FIRST).toBe('first')
    expect(process.env.STORAGE_ENV_TEST_LAST).toBe('last value')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})
