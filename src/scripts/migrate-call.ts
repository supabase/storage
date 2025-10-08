import dotenv from 'dotenv'
dotenv.config()

import { runMigrationsOnTenant } from '@internal/database/migrations'
import { getConfig } from '../config'
// eslint-disable-next-line @typescript-eslint/no-floating-promises
;(async () => {
  const { databaseURL, dbMigrationFreezeAt } = getConfig()
  await runMigrationsOnTenant({
    databaseUrl: databaseURL,
    upToMigration: dbMigrationFreezeAt,
  })
})()
