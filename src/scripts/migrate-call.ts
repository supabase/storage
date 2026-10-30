import { loadOptionalEnvFile } from '@internal/env-file'

loadOptionalEnvFile()

import { runMigrationsOnTenant } from '@internal/database/migrations'
import { getConfig } from '../config'

void (async () => {
  const { databaseURL, dbMigrationFreezeAt } = getConfig()
  await runMigrationsOnTenant({
    databaseUrl: databaseURL,
    upToMigration: dbMigrationFreezeAt,
  })
})()
