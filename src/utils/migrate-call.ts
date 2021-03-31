import dotenv from 'dotenv'
import { runMigrations } from './migrate'
dotenv.config()
;(async () => {
  await runMigrations()
})()
