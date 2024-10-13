import fastifyPlugin from 'fastify-plugin'
import { Features, getFeatures } from '@internal/database'

import { getConfig } from '../../config'

/**
 * Requires a specific feature to be enabled for a given tenant.
 *
 * This only applies for multi-tenant applications.
 * For single-tenant, use environment variables to toggle features
 * @param feature
 */
export const requireTenantFeature = (feature: keyof Features) =>
  fastifyPlugin(
    async (fastify) => {
      const { isMultitenant } = getConfig()
      fastify.addHook('onRequest', async (request, reply) => {
        if (!isMultitenant) return

        const features = await getFeatures(request.tenantId)

        if (!features[feature].enabled) {
          reply.status(403).send({
            error: 'FeatureNotEnabled',
            statusCode: '403',
            message: 'feature not enabled for this tenant',
          })
        }
      })
    },
    { name: 'tenant-feature-flags' }
  )
