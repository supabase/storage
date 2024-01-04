import dotenv from 'dotenv'
import { runMigrationsOnTenant } from '../database/migrate'
dotenv.config()
;(async () => {
  await runMigrationsOnTenant(process.env.DATABASE_URL as string)
})()
