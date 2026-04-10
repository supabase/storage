import { Knex } from 'knex'
import { randomUUID } from 'node:crypto'
import { mintJWT } from '../auth'
import { CleanupRegistry } from './types'

export interface TestUser {
  id: string
  email: string
  role: string
}

export interface CreatedUser {
  user: TestUser
  /** Pre-minted authenticated JWT for this user's `sub`. */
  jwt: string
}

export interface UserOverrides {
  id?: string
  email?: string
  role?: 'authenticated' | 'anon' | 'service_role' | string
}

export class UserFactory {
  constructor(
    private readonly db: Knex,
    private readonly prefix: string,
    private readonly registry: CleanupRegistry
  ) {}

  /**
   * Insert a single auth.users row and mint a JWT whose `sub` matches.
   *
   * Uses a single parameterized INSERT ... RETURNING so the returned row
   * reflects the real DB defaults (timestamps, etc). The row is registered
   * for teardown.
   */
  async create(overrides: UserOverrides = {}): Promise<CreatedUser> {
    const id = overrides.id ?? randomUUID()
    const email = overrides.email ?? `${this.prefix}_${id.slice(0, 8)}@test.local`
    const role = overrides.role ?? 'authenticated'

    const [row] = await this.db.raw<{ rows: TestUser[] }>(
      `INSERT INTO auth.users
         (instance_id, id, aud, role, email, raw_app_meta_data, raw_user_meta_data,
          is_super_admin, created_at, updated_at)
       VALUES ('00000000-0000-0000-0000-000000000000', ?, 'authenticated', ?, ?, '{}'::jsonb, '{}'::jsonb, false, now(), now())
       RETURNING id, email, role`,
      [id, role, email]
    ).then((r: { rows: TestUser[] }) => [r.rows[0]])

    this.registry.users.add(row.id)

    const jwt = await mintJWT({ sub: row.id, role: row.role })
    return { user: row, jwt }
  }

  /**
   * Insert many users in a single round-trip. The `generator` receives the
   * 1-based index so tests can derive stable-looking emails if they need to
   * assert on them. Each returned user comes with its own JWT.
   */
  async createMany(
    count: number,
    generator: (index: number) => UserOverrides = () => ({})
  ): Promise<CreatedUser[]> {
    if (count <= 0) return []

    const rows = Array.from({ length: count }, (_, i) => {
      const o = generator(i + 1)
      return {
        id: o.id ?? randomUUID(),
        email: o.email ?? `${this.prefix}_${i + 1}_${randomUUID().slice(0, 4)}@test.local`,
        role: o.role ?? 'authenticated',
      }
    })

    const values = rows.map(() => '(?, ?, ?)').join(', ')
    const bindings = rows.flatMap((r) => [r.id, r.role, r.email])

    await this.db.raw(
      `INSERT INTO auth.users
         (id, role, email, instance_id, aud, raw_app_meta_data, raw_user_meta_data, is_super_admin, created_at, updated_at)
       SELECT v.id::uuid, v.role, v.email, '00000000-0000-0000-0000-000000000000'::uuid, 'authenticated',
              '{}'::jsonb, '{}'::jsonb, false, now(), now()
       FROM (VALUES ${values}) AS v(id, role, email)`,
      bindings
    )

    const created: CreatedUser[] = []
    for (const r of rows) {
      this.registry.users.add(r.id)
      created.push({ user: r, jwt: await mintJWT({ sub: r.id, role: r.role }) })
    }
    return created
  }
}
