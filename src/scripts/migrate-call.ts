import dotenv from 'dotenv'
dotenv.config()

import { runMigrationsOnTenant } from '../database'
;(async () => {
  await runMigrationsOnTenant(process.env.DATABASE_URL as string)
})()
