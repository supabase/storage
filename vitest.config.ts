import { defineConfig } from 'vitest/config'
import path from 'node:path'

export default defineConfig({
  resolve: {
    tsconfigPaths: true,
    alias: {
      '@storage': path.resolve(__dirname, 'src/storage'),
      '@internal': path.resolve(__dirname, 'src/internal'),
    },
  },
  test: {
    include: ['test_v2/**/*.test.ts'],
    globals: false,
    environment: 'node',
    setupFiles: ['./test_v2/setup.ts'],
    hookTimeout: 60_000,
    testTimeout: 30_000,
    // Run each test file in its own forked process for clean tenant config /
    // OTel / pg pool state. Multiple files may run in parallel, but we cap the
    // concurrency so we don't exhaust Postgres connections.
    pool: 'forks',
    maxWorkers: 4,
    minWorkers: 1,
    // Each *file* gets a unique suffix (see helpers/context.ts) so parallel
    // files can't collide on bucket/user names. Inside a single file tests
    // still run sequentially, which avoids awkward race conditions in shared
    // tenant state (config reload, auth cache, etc.).
    fileParallelism: true,
    sequence: {
      concurrent: false,
    },
  },
})
