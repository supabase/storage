import { type CustomTypesConfig, types as defaultTypes } from 'pg'

const INT8_OID = 20

export function createPostgresTypeParsers(
  baseTypes: CustomTypesConfig = defaultTypes
): CustomTypesConfig {
  return {
    getTypeParser(oid, format) {
      if (oid === INT8_OID && (format === undefined || format === 'text')) {
        return (value: string) => Number.parseInt(value, 10)
      }

      return baseTypes.getTypeParser(oid, format)
    },
  }
}
