import { promises as fs } from 'fs'
import buildAdmin from '../admin-app'
import app from '../app'
;(async () => {
  // Export main API spec
  const storageApp = app({
    exposeDocs: true,
  })

  const response = await storageApp.inject({
    method: 'GET',
    url: '/documentation/json',
  })

  await fs.writeFile('static/api.json', response.body)

  await storageApp.close()

  // Export admin API spec
  const adminApp = buildAdmin({
    exposeDocs: true,
  })

  const adminResponse = await adminApp.inject({
    method: 'GET',
    url: '/documentation/json',
  })

  await fs.writeFile('static/api-admin.json', adminResponse.body)

  await adminApp.close()
})().catch(console.error)
