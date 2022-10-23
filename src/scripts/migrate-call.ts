import dotenv from 'dotenv'
import { runMigrations } from '../database/migrate'
dotenv.config()
;(async () => {
  await runMigrations()
})()
