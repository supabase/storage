# Database Watt PostgreSQL Ownership

Database Watt owns runtime PostgreSQL access for storage metadata and multitenant master DB queries when the app runs under Watt.

## Routed Through Database Watt

- Tenant metadata DB access via `getPostgresConnection()` when Watt messaging is available.
- Multitenant master DB access via `multitenantPgExecutor` when Watt messaging is available.

## Direct PostgreSQL Access That Remains

- Direct PostgreSQL fallback for non-Watt/local mode.
- Queue/pg-boss access in `src/internal/queue/database.ts`; queues are intentionally out of this migration for now.
- Migration runner access in `src/internal/database/migrations/migrate.ts`; migrations are intentionally out of scope for Database Watt for now.
- Tests and seeding utilities.
- `pg` type/error imports used to preserve existing public interfaces and error mapping.

Any new runtime PostgreSQL access should go through Database Watt unless it is explicitly documented here as an exception.
