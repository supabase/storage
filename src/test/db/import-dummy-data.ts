import { promises as fs } from 'fs'
import path from 'path'
import { Client } from 'pg'
;(async () => {
  const dummyDataFile = path.resolve(__dirname, './04-dummy-data.sql.sample')
  const data = await fs.readFile(dummyDataFile, 'utf-8')

  const dbConfig = {
    connectionString: process.env.DATABASE_URL,
  }
  const client = new Client(dbConfig)
  await client.connect()
  await client.query(data)
  await client.end()
})()
