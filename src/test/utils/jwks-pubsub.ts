import { TENANTS_JWKS_UPDATE_CHANNEL } from '@internal/auth/jwks/channels'
import { PostgresPubSub } from '@internal/pubsub'

export function waitForTenantJwksNotification(
  pubSub: PostgresPubSub,
  expectedTenantId: string,
  timeoutMs: number
): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(() => {
      pubSub.subscriber.notifications.removeListener(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
      reject(
        new Error(
          `Timed out after ${timeoutMs}ms waiting for ${TENANTS_JWKS_UPDATE_CHANNEL}:${expectedTenantId}`
        )
      )
    }, timeoutMs)

    const onNotification = (cacheKey: string) => {
      if (cacheKey !== expectedTenantId) {
        return
      }

      clearTimeout(timeout)
      pubSub.subscriber.notifications.removeListener(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
      resolve(cacheKey)
    }

    pubSub.subscriber.notifications.on(TENANTS_JWKS_UPDATE_CHANNEL, onNotification)
  })
}
