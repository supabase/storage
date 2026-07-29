import type { Json } from '@hyperjump/json-pointer'
import {
  registerSchema,
  setMetaSchemaOutputFormat,
  validate,
} from '@hyperjump/json-schema/openapi-3-1'
import { FastifyInstance } from 'fastify'
import buildAdmin from '../admin-app'
import buildApp from '../app'

const BASIC = 'BASIC'

setMetaSchemaOutputFormat(BASIC)

// The official OAS dialect allows unknown keywords in Schema Objects (e.g. a
// leftover OAS 3.0 `nullable: true`), so it can't catch a regression back to
// the 3.0 idiom. This strict dialect adds `unevaluatedProperties: false`,
// which - via the dialect's `$dynamicAnchor: "meta"` - rejects unknown
// keywords at every nesting level (properties, items, allOf, ...).
const STRICT_DIALECT = 'https://storage.supabase.com/oas/3.1/dialect/strict'

registerSchema(
  {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    $vocabulary: {
      'https://json-schema.org/draft/2020-12/vocab/core': true,
      'https://json-schema.org/draft/2020-12/vocab/applicator': true,
      'https://json-schema.org/draft/2020-12/vocab/unevaluated': true,
      'https://json-schema.org/draft/2020-12/vocab/validation': true,
      'https://json-schema.org/draft/2020-12/vocab/meta-data': true,
      'https://json-schema.org/draft/2020-12/vocab/format-annotation': true,
      'https://json-schema.org/draft/2020-12/vocab/content': true,
      'https://spec.openapis.org/oas/3.1/vocab/base': false,
    },
    $dynamicAnchor: 'meta',
    $ref: 'https://spec.openapis.org/oas/3.1/dialect/base',
    unevaluatedProperties: false,
  },
  STRICT_DIALECT
)

async function getSpec(instance: FastifyInstance) {
  await instance.ready()
  const response = await instance.inject({ method: 'GET', url: '/documentation/json' })
  if (response.statusCode !== 200) {
    throw new Error('Unable to get api spec: ' + response.statusCode + ' ' + response.statusMessage)
  }
  return JSON.parse(response.body)
}

// Schema Objects show up under a `schema` property (Parameter, Header, Media
// Type Objects) or as values of `components.schemas`. Nested subschemas
// (`properties`, `items`, `allOf`, ...) don't need separate entry points -
// the dialect's dynamic-scoped `meta` anchor validates them recursively as
// part of validating their containing Schema Object.
function collectSchemaObjects(
  node: Json,
  path: string,
  parentKey: string | undefined,
  out: { path: string; schema: Json }[]
) {
  if (Array.isArray(node)) {
    node.forEach((item, i) => collectSchemaObjects(item, `${path}[${i}]`, undefined, out))
    return
  }

  if (typeof node !== 'object' || node === null) {
    return
  }

  if (parentKey === 'schema') {
    out.push({ path, schema: node })
    return
  }

  if (parentKey === 'schemas') {
    for (const [name, schema] of Object.entries(node)) {
      out.push({ path: `${path}.${name}`, schema })
    }
    return
  }

  for (const [key, value] of Object.entries(node)) {
    collectSchemaObjects(value, `${path}.${key}`, key, out)
  }
}

;(async () => {
  const validateOpenApi = await validate('https://spec.openapis.org/oas/3.1/schema-base')
  const validateSchemaObjectStrict = await validate(STRICT_DIALECT)

  const specs = [
    { name: 'public API', instance: buildApp({ exposeDocs: true }) },
    { name: 'admin API', instance: buildAdmin({ exposeDocs: true }) },
  ]

  let hasErrors = false

  for (const { name, instance } of specs) {
    const spec = await getSpec(instance)
    await instance.close()

    const result = validateOpenApi(spec, BASIC)
    const specErrors: unknown[] = result.valid ? [] : (result.errors ?? [])

    const schemaObjects: { path: string; schema: Json }[] = []
    collectSchemaObjects(spec, '#', undefined, schemaObjects)

    for (const { path, schema } of schemaObjects) {
      const strictResult = validateSchemaObjectStrict(schema, BASIC)
      if (!strictResult.valid) {
        specErrors.push({ schemaObject: path, errors: strictResult.errors })
      }
    }

    if (specErrors.length > 0) {
      hasErrors = true
      console.error(`✗ ${name} OpenAPI spec is not valid OpenAPI 3.1:`)
      console.error(JSON.stringify(specErrors, null, 2))
    } else {
      console.log(`✓ ${name} OpenAPI spec is valid OpenAPI 3.1`)
    }
  }

  if (hasErrors) {
    process.exit(1)
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
