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
