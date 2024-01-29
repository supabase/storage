import dotenv from 'dotenv'
dotenv.config()

import { runMigrationsOnTenant } from '../database/migrate'
;(async () => {
  await runMigrationsOnTenant(process.env.DATABASE_URL as string)
})()
