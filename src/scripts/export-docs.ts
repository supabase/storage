import { promises as fs } from 'fs'
import app from '../app'
;(async () => {
  const storageApp = app({
    exposeDocs: true,
  })

  const response = await storageApp.inject({
    method: 'GET',
    url: '/documentation/json',
  })

  await fs.writeFile('static/api.json', response.body)

  await storageApp.close()
})()
