import { promises as fs } from 'fs'
import app from '../app'
;(async () => {
  const response = await app({
    exposeDocs: true,
  }).inject({
    method: 'GET',
    url: '/documentation/json',
  })

  await fs.writeFile('static/api.json', response.body)
})()
