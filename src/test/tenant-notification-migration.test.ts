import { randomUUID } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { Client } from 'pg'
import { getConfig } from '../config'

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
