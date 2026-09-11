import dotenv from 'dotenv'

dotenv.config()

import { resetMigration, runMigrationsOnTenant } from '@internal/database/migrations'
import { DBMigration } from '@internal/database/migrations/types'
import { getConfig } from '../config'

void (async () => {
  const { databaseURL, dbMigrationFreezeAt } = getConfig()
  const migrations = Object.keys(DBMigration) as (keyof typeof DBMigration)[]

  let previousMigration: keyof typeof DBMigration = 'create-migrations-table'

  for (const migration of migrations.slice(1)) {
    console.log(`Running   migration ${migration}`)
    await runMigrationsOnTenant({
      databaseUrl: databaseURL,
      upToMigration: migration,
    })

    console.log(`Resetting migration ${migration}`)
    await resetMigration({
      databaseUrl: databaseURL,
      untilMigration: previousMigration,
    })

    console.log(`Rerunning migration ${migration}`)
    await runMigrationsOnTenant({
      databaseUrl: databaseURL,
      upToMigration: migration,
    })

    if (dbMigrationFreezeAt === migration) {
      break
    }

    previousMigration = migration
  }
})()
