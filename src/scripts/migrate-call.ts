import dotenv from 'dotenv'
import { runMigrations } from '../database/migrate'
dotenv.config()
;(async () => {
  try {
    await runMigrations()
  } catch (e) {
    console.error(e)
    process.exit(1)
  }
})()
