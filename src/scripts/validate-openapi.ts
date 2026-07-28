import { setMetaSchemaOutputFormat, validate } from '@hyperjump/json-schema/openapi-3-2'
import { FastifyInstance } from 'fastify'
import buildAdmin from '../admin-app'
import buildApp from '../app'

const BASIC = 'BASIC'

setMetaSchemaOutputFormat(BASIC)

async function getSpec(instance: FastifyInstance) {
  await instance.ready()
  const response = await instance.inject({ method: 'GET', url: '/documentation/json' })
  if (response.statusCode !== 200) {
    throw new Error('Unable to get api spec: ' + response.statusCode + ' ' + response.statusMessage)
  }
  return JSON.parse(response.body)
}

;(async () => {
  const validateOpenApi = await validate('https://spec.openapis.org/oas/3.2/schema')

  const specs = [
    { name: 'public API', instance: buildApp({ exposeDocs: true }) },
    { name: 'admin API', instance: buildAdmin({ exposeDocs: true }) },
  ]

  let hasErrors = false

  for (const { name, instance } of specs) {
    const spec = await getSpec(instance)
    const result = validateOpenApi(spec, BASIC)
    await instance.close()

    if (!result.valid) {
      hasErrors = true
      console.error(`✗ ${name} OpenAPI spec is not valid OpenAPI 3.2:`)
      console.error(JSON.stringify(result, null, 2))
    } else {
      console.log(`✓ ${name} OpenAPI spec is valid OpenAPI 3.2`)
    }
  }

  if (hasErrors) {
    process.exit(1)
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
