import dotenv from 'dotenv'
import { runMigrations } from '../utils/migrate'
dotenv.config()
;(async () => {
  await runMigrations()
})()
