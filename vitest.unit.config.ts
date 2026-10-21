import { availableParallelism } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const rootDir = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  resolve: {
    alias: {
      '@internal': path.resolve(rootDir, 'src/internal'),
      '@storage': path.resolve(rootDir, 'src/storage'),
    },
  },
  test: {
    fsModuleCachePath: path.resolve(rootDir, '.cache/vitest'),
    coverage: {
      provider: 'v8',
      reporter: ['lcovonly', 'text-summary'],
    },
    environment: 'node',
    globals: true,
    include: ['src/**/*.test.ts', 'acceptance/support/**/*.test.ts'],
    exclude: ['src/test/**/*.test.ts'],
    maxWorkers: Math.min(4, Math.max(1, availableParallelism() - 1)),
    clearMocks: true,
    restoreMocks: true,
    unstubEnvs: true,
    unstubGlobals: true,
  },
})
