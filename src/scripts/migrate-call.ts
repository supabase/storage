import dotenv from 'dotenv'
dotenv.config()

import { runMigrationsOnTenant } from '@internal/database/migrations'
;(async () => {
  await runMigrationsOnTenant(process.env.DATABASE_URL as string)
})()
