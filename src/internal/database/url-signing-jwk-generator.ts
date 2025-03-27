import { logger, logSchema } from '../monitoring'
import { JwksCreateSigningSecret } from '@storage/events'
import { listTenantsMissingUrlSigningJwk } from './tenant'
import { getConfig } from '../../config'

const { isMultitenant, pgQueueEnable } = getConfig()

export interface UrlSigningJwkGeneratorStatus {
  running: boolean
  sent: number
}

export class UrlSigningJwkGenerator {
  private static signal: AbortSignal
  private static isRunning: boolean = false
  private static countSent: number = 0

  static Init(signal: AbortSignal) {
    UrlSigningJwkGenerator.signal = signal
  }

  static getGenerationStatus(): UrlSigningJwkGeneratorStatus {
    return {
      running: UrlSigningJwkGenerator.isRunning,
      sent: UrlSigningJwkGenerator.countSent,
    }
  }

  /**
   * Generates url signing jwks for all tenants
   */
  static async generateUrlSigningJwksOnAllTenants() {
    if (!pgQueueEnable || !isMultitenant || UrlSigningJwkGenerator.isRunning) {
      return
    }
    UrlSigningJwkGenerator.isRunning = true
    UrlSigningJwkGenerator.countSent = 0
    logSchema.info(logger, '[Jwks Generator] Generating url signing jwks for all tenants', {
      type: 'jwk-generator',
    })
    try {
      const tenants = listTenantsMissingUrlSigningJwk(UrlSigningJwkGenerator.signal)
      for await (const tenantBatch of tenants) {
        await JwksCreateSigningSecret.batchSend(
          tenantBatch.map((tenant) => {
            return new JwksCreateSigningSecret({
              tenantId: tenant,
              tenant: {
                host: '',
                ref: tenant,
              },
            })
          })
        )
        UrlSigningJwkGenerator.countSent += tenantBatch.length
      }

      logSchema.info(
        logger,
        `[Jwks Generator] Completed generation of url signing jwks for ${UrlSigningJwkGenerator.countSent} tenants`,
        {
          type: 'jwk-generator',
        }
      )
    } catch (e) {
      logSchema.error(logger, '[Jwks Generator] Error generating url signing jwks', {
        type: 'jwk-generator',
        error: e,
        metadata: JSON.stringify({
          completed: UrlSigningJwkGenerator.countSent,
        }),
      })
    }
    UrlSigningJwkGenerator.isRunning = false
  }
}
