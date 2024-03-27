import { FastifyInstance } from 'fastify'
import accepts from '@fastify/accepts'
import fastifyPlugin from 'fastify-plugin'
import xml from 'xml2js'

// no types exists for this package
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import xmlBodyParser from 'fastify-xml-body-parser'

export const jsonToXml = fastifyPlugin(async function (fastify: FastifyInstance) {
  fastify.register(accepts)

  fastify.register(xmlBodyParser)
  fastify.addHook('preSerialization', async (req, res, payload) => {
    const accept = req.accepts()
    if (
      res.getHeader('content-type')?.toString()?.includes('application/json') &&
      accept.types(['application/xml', 'application/json']) === 'application/xml'
    ) {
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

  fastify.addHook('onSend', async (req, res, payload) => {
    if (res.getHeader('content-type')?.toString()?.includes('application/xml')) {
      return JSON.parse(payload as any)
    }
    return payload
  })
})
