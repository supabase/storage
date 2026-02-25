import accepts from '@fastify/accepts'
import { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import xml from 'xml2js'

type XmlParserOptions = { disableContentParser?: boolean; parseAsArray?: string[] }
type RequestError = Error & { statusCode?: number }

export function decodeXmlNumericEntities(value: string): string {
  return value.replace(/&#([xX][0-9a-fA-F]{1,6}|[0-9]{1,7});/g, (match: string, rawValue: string) => {
    const isHex = rawValue[0].toLowerCase() === 'x'
    const codePoint = Number.parseInt(isHex ? rawValue.slice(1) : rawValue, isHex ? 16 : 10)
    if (codePoint > 0x10ffff) {
      return match
    }

    return String.fromCodePoint(codePoint)
  })
}

function forcePathAsArray(node: unknown, pathSegments: string[]): void {
  if (pathSegments.length === 0 || node === null || node === undefined) {
    return
  }

  if (Array.isArray(node)) {
    node.forEach((item) => forcePathAsArray(item, pathSegments))
    return
  }

  if (typeof node !== 'object') {
    return
  }

  const [current, ...rest] = pathSegments
  const currentRecord = node as Record<string, unknown>

  if (!(current in currentRecord)) {
    return
  }

  if (rest.length === 0) {
    const value = currentRecord[current]
    if (value !== undefined && !Array.isArray(value)) {
      currentRecord[current] = [value]
    }
    return
  }

  forcePathAsArray(currentRecord[current], rest)
}

export const xmlParser = fastifyPlugin(
  async function (fastify: FastifyInstance, opts: XmlParserOptions) {
    fastify.register(accepts)

    if (!opts.disableContentParser) {
      fastify.addContentTypeParser(
        ['text/xml', 'application/xml'],
        { parseAs: 'string' },
        (_request, body, done) => {
          if (!body) {
            done(null, null)
            return
          }

          xml.parseString(
            body,
            {
              explicitArray: false,
              trim: true,
              valueProcessors: [
                decodeXmlNumericEntities,
                xml.processors.parseNumbers,
                xml.processors.parseBooleans,
              ],
            },
            (err: Error | null, parsed: unknown) => {
              if (err) {
                const parseError: RequestError = new Error(`Invalid XML payload: ${err.message}`)
                parseError.statusCode = 400
                done(parseError)
                return
              }

              if (parsed && opts.parseAsArray?.length) {
                opts.parseAsArray.forEach((path) => {
                  if (!path) {
                    return
                  }
                  forcePathAsArray(parsed, path.split('.'))
                })
              }

              done(null, parsed)
            }
          )
        }
      )
    }

    fastify.addHook('preSerialization', async (req, res, payload) => {
      const accept = req.accepts()

      const acceptedTypes = ['application/xml', 'text/html']

      if (acceptedTypes.some((allowed) => accept.types(acceptedTypes) === allowed)) {
        res.serializer((payload) => payload)

        const xmlBuilder = new xml.Builder({
          renderOpts: {
            pretty: false,
          },
        })
        const xmlPayload = xmlBuilder.buildObject(payload)
        res.type('application/xml')
        res.header('content-type', 'application/xml; charset=utf-8')
        return xmlPayload
      }

      return payload
    })
  },
  { name: 'xml-parser' }
)
