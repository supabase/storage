import { afterAll, afterEach, beforeAll, beforeEach } from 'vitest'
import { FastifyInstance } from 'fastify'
import { Knex } from 'knex'
import app from '../../../app'
import { mintJWT, serviceKey } from './auth'
import { makeClient, TestClient } from './client'
import { disposeTestKnex, getTestKnex, withDeleteEnabled } from './db'
import { BucketFactory } from './factories/bucket'
import { ObjectFactory } from './factories/object'
import { TestBucket } from './factories/bucket'
import { CleanupRegistry, createRegistry } from './factories/types'
import { UserFactory } from './factories/user'
import { makeFilePrefix } from './random'
import { deleteS3PrefixesForBuckets, ensureRootBucket } from './s3'
import { Snapshot } from './snapshot'

export interface TestContext {
  /**
   * The fastify app under test. A *fresh* instance is built per test in
   * beforeEach so plugins / decorators don't leak across tests, then closed
   * in afterEach. Tests just call `ctx.app.inject(...)`.
   */
  readonly app: FastifyInstance
  /**
   * Shortcut for the common `app.inject(...)` patterns. Prefer this over
   * `ctx.app.inject` directly — it hides the `authorization: Bearer ...`
   * boilerplate for service / user / anon / unauthenticated callers.
   */
  readonly client: TestClient
  /** Postgres-superuser knex used by factories. NOT the same connection the app uses. */
  readonly db: Knex
  /** Per-file random prefix — useful when a test wants to namespace its own names. */
  readonly prefix: string
  readonly factories: {
    user: UserFactory
    bucket: BucketFactory
    /**
     * Build an object factory bound to a bucket. Pass either the TestBucket
     * returned by `factories.bucket.create()` or just its id.
     */
    objectsIn(bucket: TestBucket): ObjectFactory
  }
  /**
   * Manually register resources for cleanup. Use this when the *test code*
   * (not a factory) creates a bucket / user / object — typically because the
   * test wants to exercise the HTTP create endpoint and still get teardown.
   */
  readonly track: {
    bucket(id: string): void
    user(id: string): void
    s3Key(key: string): void
  }
  /** Snapshot-style row assertions ("after this API call, the row looks like..."). */
  readonly snapshot: Snapshot
  /** Helper for tests that need a service-role bearer token. */
  readonly serviceJwt: () => Promise<string>
  /** Helper for tests that need a user-bound bearer token without going through user.create. */
  readonly mintJwt: typeof mintJWT
}

export interface UseTestContextOptions {
  /**
   * Set to true if the test file actually uploads to / reads from MinIO. We
   * only ensure the root S3 bucket exists when needed — pure DB-level tests
   * skip the round-trip.
   */
  s3?: boolean
}

/**
 * Wires up vitest hooks for a test file. Call once at the top of every spec:
 *
 *   const ctx = useTestContext({ s3: true })
 *   test('...', async () => { await ctx.app.inject(...) })
 *
 * Lifecycle (in order):
 *   beforeAll  — open shared knex, ensure S3 root bucket if requested
 *   beforeEach — fresh fastify app
 *   afterEach  — close fastify app
 *   afterAll   — bulk-delete every row this file inserted, then dispose knex
 */
export function useTestContext(options: UseTestContextOptions = {}): TestContext {
  const prefix = makeFilePrefix()
  const registry: CleanupRegistry = createRegistry()
  let appInstance: FastifyInstance | undefined
  let db: Knex | undefined

  beforeAll(async () => {
    db = getTestKnex()
    if (options.s3) {
      await ensureRootBucket()
    }
  })

  beforeEach(() => {
    appInstance = app()
  })

  afterEach(async () => {
    if (appInstance) {
      await appInstance.close()
      appInstance = undefined
    }
  })

  afterAll(async () => {
    if (!db) return
    try {
      await teardown(db, registry, options.s3 === true)
    } finally {
      await disposeTestKnex()
    }
  })

  const getApp = (): FastifyInstance => {
    if (!appInstance) {
      throw new Error('ctx.app accessed outside a test (no beforeEach hook ran yet)')
    }
    return appInstance
  }
  const client = makeClient(getApp)

  const ctx: TestContext = {
    get app() {
      return getApp()
    },
    client,
    get db() {
      if (!db) {
        throw new Error('ctx.db accessed before beforeAll ran')
      }
      return db
    },
    prefix,
    factories: {
      get user() {
        return new UserFactory(getTestKnex(), prefix, registry)
      },
      get bucket() {
        return new BucketFactory(getTestKnex(), prefix, registry)
      },
      objectsIn(bucket) {
        return new ObjectFactory(getTestKnex(), bucket, registry)
      },
    },
    track: {
      bucket(id) {
        registry.buckets.add(id)
      },
      user(id) {
        registry.users.add(id)
      },
      s3Key(key) {
        registry.s3Keys.add(key)
      },
    },
    get snapshot() {
      return new Snapshot(getTestKnex())
    },
    serviceJwt: serviceKey,
    mintJwt: mintJWT,
  }

  return ctx
}

/**
 * Bulk teardown for everything a file created. Each table gets at most ONE
 * DELETE statement, so cleanup cost is O(1) in the number of test files
 * regardless of how many rows were inserted.
 */
async function teardown(
  db: Knex,
  registry: CleanupRegistry,
  cleanS3: boolean
): Promise<void> {
  const bucketIds = [...registry.buckets]
  const userIds = [...registry.users]

  if (bucketIds.length > 0) {
    await withDeleteEnabled(db, async (trx) => {
      // Objects first (FK to buckets), then buckets.
      await trx('storage.objects').whereIn('bucket_id', bucketIds).del()
      await trx('storage.buckets').whereIn('id', bucketIds).del()
    })
  }

  if (userIds.length > 0) {
    await db('auth.users').whereIn('id', userIds).del()
  }

  if (cleanS3 && bucketIds.length > 0) {
    try {
      await deleteS3PrefixesForBuckets(bucketIds)
    } catch (err) {
      // Don't fail the suite if MinIO is unhappy — just log it. The next
      // infra:restart will wipe state anyway.
      // eslint-disable-next-line no-console
      console.warn('[test_v2] S3 prefix cleanup failed:', err)
    }
  }
}
