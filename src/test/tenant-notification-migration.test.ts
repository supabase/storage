import { randomUUID } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { Client } from 'pg'
import { getConfig, mergeConfig } from '../config'

// The upgrade fixture needs to create a disposable database.
describe.skipIf(getConfig().databaseEngine === 'multigres')('tenant notification migration', () => {
  const name = `tenant_notify_${randomUUID().replaceAll('-', '')}`
  const url = new URL(getConfig().databaseURL)
  url.pathname = `/${name}`
  const admin = new Client({ connectionString: getConfig().databaseURL })
  let writer: Client
  let listener: Client

  beforeAll(async () => {
    await admin.connect()
    await admin.query(`CREATE DATABASE ${name}`)
    writer = new Client({ connectionString: url.toString() })
    listener = new Client({ connectionString: url.toString() })
    await Promise.all([writer.connect(), listener.connect()])
    await writer.query("SET storage.iceberg_shards = '{}'")
    const directory = 'migrations/multitenant'
    for (const file of (await readdir(directory))
      .filter((entry) => entry.endsWith('.sql') && entry < '0032')
      .sort()) {
      await writer.query(await readFile(`${directory}/${file}`, 'utf8'))
    }
    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key, jwks)
      VALUES ('notify-test', 'anon', 'url', 'secret', 'service', '{"keys":[]}'::jsonb)
    `)
    await listener.query('LISTEN tenants_update')
  })

  afterAll(async () => {
    await Promise.all([writer?.end(), listener?.end()])
    await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
    await admin.end()
  })

  it('notifies only for changed updates and keeps delete and transaction behavior', async () => {
    const migration = await readFile(
      'migrations/multitenant/0032-notify-changed-tenants.sql',
      'utf8'
    )

    const { TenantConfigStorePg } = await import('@internal/database/tenant-store-pg')
    const store = new TenantConfigStorePg({
      query: (statement: { text: string; values?: unknown[] }) => writer.query(statement),
    } as unknown as ConstructorParameters<typeof TenantConfigStorePg>[0])
    await expect(store.findById('notify-test')).resolves.toMatchObject({
      id: 'notify-test',
      updated_at_us: null,
    })

    // An existing listener continues to receive the same payload across the upgrade.
    expect(
      await notificationsFor("UPDATE tenants SET anon_key = 'old' WHERE id = 'notify-test'")
    ).toEqual(['"notify-test"'])
    await writer.query(migration)
    await writer.query(migration)

    expect(
      await notificationsFor("UPDATE tenants SET anon_key = anon_key WHERE id = 'notify-test'")
    ).toEqual([])
    expect(
      await notificationsFor("UPDATE tenants SET anon_key = 'new' WHERE id = 'notify-test'")
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        "UPDATE tenants SET jwks = '{\"keys\":[]}'::jsonb WHERE id = 'notify-test'"
      )
    ).toEqual([])
    expect(
      await notificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'notify-test'"
      )
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'notify-test'"
      )
    ).toEqual([])
    expect(
      await notificationsFor("UPDATE tenants SET migrations_status = NULL WHERE id = 'notify-test'")
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        "UPDATE tenants SET jwks = '{\"keys\":[1]}'::jsonb WHERE id = 'notify-test'"
      )
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        "UPDATE tenants SET disable_events = ARRAY['a'] WHERE id = 'notify-test'"
      )
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        "UPDATE tenants SET disable_events = ARRAY['a'] WHERE id = 'notify-test'"
      )
    ).toEqual([])
    expect(
      await notificationsFor(
        "BEGIN; UPDATE tenants SET anon_key = 'rollback' WHERE id = 'notify-test'; ROLLBACK"
      )
    ).toEqual([])

    // Future columns need not provide an equality operator (unlike jsonb, json does not).
    await writer.query('ALTER TABLE tenants ADD COLUMN future_config json')
    expect(
      await notificationsFor("UPDATE tenants SET anon_key = anon_key WHERE id = 'notify-test'")
    ).toEqual([])
    expect(
      await notificationsFor(
        `UPDATE tenants SET future_config = '{"enabled":true}'::json WHERE id = 'notify-test'`
      )
    ).toEqual(['"notify-test"'])
    expect(
      await notificationsFor(
        `UPDATE tenants SET future_config = '{"enabled":true}'::json WHERE id = 'notify-test'`
      )
    ).toEqual([])
    expect(
      await notificationsFor(
        `UPDATE tenants SET future_config = '{"enabled":false}'::json WHERE id = 'notify-test'`
      )
    ).toEqual(['"notify-test"'])
    expect(await notificationsFor("DELETE FROM tenants WHERE id = 'notify-test'")).toEqual([
      '"notify-test"',
    ])
  })

  it('maintains updated_at without notifying for unchanged tenant rows', async () => {
    await writer.query(
      await readFile('migrations/multitenant/0032-notify-changed-tenants.sql', 'utf8')
    )
    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
      VALUES ('timestamp-test', 'anon', 'url', 'secret', 'service')
    `)
    const migration = await readFile('migrations/multitenant/0033-tenants-updated-at.sql', 'utf8')
    await writer.query(migration)
    await writer.query(migration)

    async function timestamp(id: string): Promise<string | null> {
      const result = await writer.query<{ updated_at: string | null }>(
        'SELECT updated_at::text FROM tenants WHERE id = $1',
        [id]
      )
      return result.rows[0].updated_at
    }

    expect(await timestamp('timestamp-test')).toBeNull()
    expect(
      await notificationsFor("UPDATE tenants SET anon_key = anon_key WHERE id = 'timestamp-test'")
    ).toEqual([])
    expect(await timestamp('timestamp-test')).toBeNull()

    expect(
      await notificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'timestamp-test'"
      )
    ).toEqual(['"timestamp-test"'])
    const first = await timestamp('timestamp-test')
    expect(first).not.toBeNull()
    expect(
      await notificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'timestamp-test'"
      )
    ).toEqual([])
    expect(await timestamp('timestamp-test')).toBe(first)

    await writer.query('SELECT pg_sleep(0.005)')
    expect(
      await notificationsFor("UPDATE tenants SET anon_key = 'new' WHERE id = 'timestamp-test'")
    ).toEqual(['"timestamp-test"'])
    const second = await timestamp('timestamp-test')
    expect(second).not.toBe(first)

    await writer.query('BEGIN')
    const transactionFirst = await writer.query<{ updated_at: string }>(
      "UPDATE tenants SET anon_key = 'transaction-1' WHERE id = 'timestamp-test' RETURNING updated_at::text"
    )
    await writer.query('SELECT pg_sleep(0.005)')
    const transactionSecond = await writer.query<{ updated_at: string }>(
      "UPDATE tenants SET anon_key = 'transaction-2' WHERE id = 'timestamp-test' RETURNING updated_at::text"
    )
    expect(transactionSecond.rows[0].updated_at).not.toBe(transactionFirst.rows[0].updated_at)
    await writer.query('ROLLBACK')
    expect(await timestamp('timestamp-test')).toBe(second)

    expect(
      await notificationsFor(
        "BEGIN; UPDATE tenants SET anon_key = 'rolled-back' WHERE id = 'timestamp-test'; ROLLBACK"
      )
    ).toEqual([])
    expect(await timestamp('timestamp-test')).toBe(second)

    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
      VALUES ('timestamp-new', 'anon', 'url', 'secret', 'service')
    `)
    expect(await timestamp('timestamp-new')).not.toBeNull()

    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key, updated_at)
      VALUES ('timestamp-ahead', 'anon', 'url', 'secret', 'service', clock_timestamp() + interval '1 hour')
    `)
    const ahead = await timestamp('timestamp-ahead')
    await writer.query("UPDATE tenants SET anon_key = 'new' WHERE id = 'timestamp-ahead'")
    const advanced = await writer.query<{ next: boolean }>(
      "SELECT updated_at = $1::timestamptz + interval '1 microsecond' AS next FROM tenants WHERE id = 'timestamp-ahead'",
      [ahead]
    )
    expect(advanced.rows).toEqual([{ next: true }])

    await writer.query('ALTER TABLE tenants ADD COLUMN IF NOT EXISTS timestamp_future_config json')
    expect(
      await notificationsFor(
        "UPDATE tenants SET timestamp_future_config = '{\"enabled\":true}'::json WHERE id = 'timestamp-test'"
      )
    ).toEqual(['"timestamp-test"'])
    const afterJson = await timestamp('timestamp-test')
    expect(
      await notificationsFor(
        "UPDATE tenants SET timestamp_future_config = '{\"enabled\":true}'::json WHERE id = 'timestamp-test'"
      )
    ).toEqual([])
    expect(await timestamp('timestamp-test')).toBe(afterJson)
  })

  it('routes migration-only updates separately after the listener rollout', async () => {
    await writer.query(await readFile('migrations/multitenant/0033-tenants-updated-at.sql', 'utf8'))
    await writer.query('ALTER TABLE tenants ADD COLUMN IF NOT EXISTS future_config json')
    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
      VALUES ('split-test', 'anon', 'url', 'secret', 'service')
    `)
    await listener.query('LISTEN tenants_migrations_update')
    const migration = await readFile(
      'migrations/rollout/tenant-migration-notifications.sql',
      'utf8'
    )
    await writer.query(migration)
    await writer.query(migration)

    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_migrations_update'])
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'split-test'"
      )
    ).toEqual([])
    expect(
      await splitNotificationsFor("UPDATE tenants SET anon_key = 'new' WHERE id = 'split-test'")
    ).toEqual(['tenants_update'])
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET anon_key = 'other', migrations_version = 'storage-schema' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_update', 'tenants_migrations_update'])
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET future_config = '{\"x\":1}'::json WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_update'])
    expect(
      await splitNotificationsFor(
        "BEGIN; UPDATE tenants SET migrations_status = NULL WHERE id = 'split-test'; ROLLBACK"
      )
    ).toEqual([])
    // Row-type changes after the function has run in this session keep routing correct.
    await writer.query('ALTER TABLE tenants ADD COLUMN later_config json')
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET later_config = '{\"x\":1}'::json WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_update'])
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET migrations_status = 'COMPLETED' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_migrations_update'])
    await writer.query('ALTER TABLE tenants DROP COLUMN later_config')
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET anon_key = 'after-drop' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_update'])
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET migrations_version = 'initialmigration' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_migrations_update'])

    const repeated = await splitNotificationPayloadsFor(`
      BEGIN;
      UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'split-test';
      UPDATE tenants SET migrations_status = 'COMPLETED' WHERE id = 'split-test';
      UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'split-test';
      COMMIT;
    `)
    expect(repeated).toHaveLength(3)
    expect(
      repeated.map((message) => (message.payload as { migrationStatus: string }).migrationStatus)
    ).toEqual(['FAILED', 'COMPLETED', 'FAILED'])
    expect(
      new Set(repeated.map((message) => (message.payload as { updatedAtUs: string }).updatedAtUs))
        .size
    ).toBe(3)
    const timestamp = await writer.query<{ updated_at_us: string }>(
      "SELECT round(extract(epoch FROM updated_at) * 1000000)::bigint::text AS updated_at_us FROM tenants WHERE id = 'split-test'"
    )
    expect(repeated[2].payload).toMatchObject({ updatedAtUs: timestamp.rows[0].updated_at_us })

    const mixed = await splitNotificationPayloadsFor(`
      BEGIN;
      UPDATE tenants SET anon_key = 'config-before' WHERE id = 'split-test';
      UPDATE tenants SET migrations_status = 'COMPLETED' WHERE id = 'split-test';
      UPDATE tenants SET anon_key = 'config-after', migrations_status = 'FAILED' WHERE id = 'split-test';
      COMMIT;
    `)
    expect(mixed.map((message) => message.channel)).toEqual([
      'tenants_update',
      'tenants_migrations_update',
      'tenants_migrations_update',
    ])
    expect(mixed[2].payload).toMatchObject({
      type: 'update',
      tenantId: 'split-test',
      migrationStatus: 'FAILED',
    })

    const recreate = await splitNotificationPayloadsFor(`
      BEGIN;
      UPDATE tenants SET anon_key = 'before-delete' WHERE id = 'split-test';
      UPDATE tenants SET migrations_status = 'COMPLETED' WHERE id = 'split-test';
      DELETE FROM tenants WHERE id = 'split-test';
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
      VALUES ('split-test', 'new', 'url', 'secret', 'service');
      COMMIT;
    `)
    expect(recreate.map((message) => message.channel)).toEqual([
      'tenants_update',
      'tenants_migrations_update',
      'tenants_migrations_update',
    ])
    expect(recreate[2].payload).toMatchObject({ type: 'delete', tenantId: 'split-test' })

    const oversized = await splitNotificationPayloadsFor(
      "UPDATE tenants SET migrations_version = repeat('x', 9000) WHERE id = 'split-test'"
    )
    expect(oversized).toEqual([
      {
        channel: 'tenants_migrations_update',
        payload: expect.objectContaining({ type: 'invalidate', tenantId: 'split-test' }),
      },
    ])

    expect(await splitNotificationsFor("DELETE FROM tenants WHERE id = 'split-test'")).toEqual([
      'tenants_update',
      'tenants_migrations_update',
    ])

    const escapedId = 'split"\\test'
    await writer.query(
      `INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
       VALUES ($1, 'anon', 'url', 'secret', 'service')`,
      [escapedId]
    )
    expect(
      await splitNotificationPayloadsFor(
        `UPDATE tenants SET anon_key = 'new' WHERE id = 'split"\\test'`,
        escapedId
      )
    ).toEqual([{ channel: 'tenants_update', payload: escapedId }])
    expect(
      await splitNotificationPayloadsFor(`DELETE FROM tenants WHERE id = 'split"\\test'`, escapedId)
    ).toEqual([
      { channel: 'tenants_update', payload: escapedId },
      {
        channel: 'tenants_migrations_update',
        payload: expect.objectContaining({ type: 'delete', tenantId: escapedId }),
      },
    ])

    // Restoring the legacy trigger makes an old listener safe before rollback.
    await writer.query(`
      INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key)
      VALUES ('split-test', 'anon', 'url', 'secret', 'service')
    `)
    await writer.query(
      await readFile('migrations/multitenant/0032-notify-changed-tenants.sql', 'utf8')
    )
    expect(
      await splitNotificationsFor(
        "UPDATE tenants SET migrations_status = 'FAILED' WHERE id = 'split-test'"
      )
    ).toEqual(['tenants_update'])
  })

  it('updates cached migration state through PostgreSQL notifications', async () => {
    const { encrypt } = await import('@internal/auth')
    const { PostgresPubSub } = await import('@internal/pubsub')
    const tenant = await import('@internal/database/tenant')
    const { closeMultitenantPg, multitenantPgExecutor } = await import('@internal/database')
    const connectionString = url.toString()
    const pubsub = new PostgresPubSub(connectionString)
    pubsub.on('error', () => {})
    mergeConfig({
      multitenantDatabaseUrl: connectionString,
      multitenantDatabasePoolUrl: connectionString,
    })
    await writer.query(await readFile('migrations/multitenant/0033-tenants-updated-at.sql', 'utf8'))
    await writer.query(
      await readFile('migrations/rollout/tenant-migration-notifications.sql', 'utf8')
    )
    await writer.query(
      `INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key, migrations_version, migrations_status)
       VALUES ($1, $2, $3, $4, $5, 'initialmigration', 'FAILED')`,
      ['live-test', encrypt('anon'), encrypt('url'), encrypt('secret'), encrypt('service')]
    )

    try {
      await pubsub.start()
      await tenant.listenForTenantUpdate(pubsub)
      const config = await tenant.getTenantConfig('live-test')
      config.syncMigrationsDone = true
      const previousUpdatedAtUs = config.updatedAtUs
      const query = vi.spyOn(multitenantPgExecutor, 'query')

      let barrier = Promise.withResolvers<void>()
      await pubsub.subscribe('tenant_test_barrier', () => barrier.resolve())
      await writer.query(
        "UPDATE tenants SET migrations_version = 'storage-schema', migrations_status = 'COMPLETED' WHERE id = 'live-test'"
      )
      await writer.query("SELECT pg_notify('tenant_test_barrier', $1)", [JSON.stringify('done')])
      await barrier.promise

      expect(await tenant.getTenantConfig('live-test')).toBe(config)
      expect(config).toMatchObject({
        migrationVersion: 'storage-schema',
        migrationStatus: 'COMPLETED',
      })
      expect(config.syncMigrationsDone).toBe(false)
      expect(query).not.toHaveBeenCalled()

      barrier = Promise.withResolvers<void>()
      await writer.query("SELECT pg_notify('tenants_migrations_update', $1)", [
        JSON.stringify({
          type: 'update',
          tenantId: 'live-test',
          migrationVersion: 'initialmigration',
          migrationStatus: 'FAILED',
          updatedAtUs: previousUpdatedAtUs,
        }),
      ])
      await writer.query("SELECT pg_notify('tenant_test_barrier', $1)", [JSON.stringify('done')])
      await barrier.promise
      expect(config).toMatchObject({
        migrationVersion: 'storage-schema',
        migrationStatus: 'COMPLETED',
      })
      expect(query).not.toHaveBeenCalled()
    } finally {
      tenant.deleteTenantConfig('live-test')
      await pubsub.close()
      await closeMultitenantPg()
    }
  })

  async function splitNotificationsFor(sql: string): Promise<string[]> {
    return (await splitNotificationPayloadsFor(sql)).map((message) => message.channel)
  }

  async function splitNotificationPayloadsFor(
    sql: string,
    tenantId = 'split-test'
  ): Promise<{ channel: string; payload: unknown }[]> {
    const marker = JSON.stringify(`marker-${randomUUID()}`)
    const messages: { channel: string; payload: unknown }[] = []
    let resolveMarker!: () => void
    const markerSeen = new Promise<void>((resolve) => {
      resolveMarker = resolve
    })
    const onNotification = (message: { channel: string; payload?: string }) => {
      if (message.payload === marker) resolveMarker()
      else if (message.payload) {
        const payload: unknown = JSON.parse(message.payload)
        if (
          payload === tenantId ||
          (payload &&
            typeof payload === 'object' &&
            'tenantId' in payload &&
            payload.tenantId === tenantId)
        ) {
          messages.push({ channel: message.channel, payload })
        }
      }
    }
    listener.on('notification', onNotification)
    try {
      await writer.query(sql)
      await writer.query("SELECT pg_notify('tenants_update', $1)", [marker])
      await markerSeen
      return messages
    } finally {
      listener.off('notification', onNotification)
    }
  }

  async function notificationsFor(sql: string): Promise<string[]> {
    const marker = JSON.stringify(`marker-${randomUUID()}`)
    const messages: string[] = []
    let resolveMarker!: () => void
    const markerSeen = new Promise<void>((resolve) => {
      resolveMarker = resolve
    })
    const onNotification = (message: { channel: string; payload?: string }) => {
      if (message.channel !== 'tenants_update') return
      if (message.payload === marker) resolveMarker()
      else if (message.payload !== undefined) messages.push(message.payload)
    }
    listener.on('notification', onNotification)
    try {
      await writer.query(sql)
      // Notifications are ordered at commit; this marker is a deterministic delivery barrier.
      await writer.query("SELECT pg_notify('tenants_update', $1)", [marker])
      await markerSeen
      return messages
    } finally {
      listener.off('notification', onNotification)
    }
  }
})
