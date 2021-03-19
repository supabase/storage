import { runMigrations } from './migrate'
import dotenv from 'dotenv'
dotenv.config()
;(async () => {
  await runMigrations()
})()
