import dotenv from 'dotenv'
dotenv.config()

import { getConfig, runMigrationsOnTenant } from '@internal/database/migrations'
;(async () => {
  const { databaseURL, dbMigrationFreezeAt } = getConfig()
  await runMigrationsOnTenant({
    databaseUrl: databaseURL,
    upToMigration: dbMigrationFreezeAt,
  })
})()
