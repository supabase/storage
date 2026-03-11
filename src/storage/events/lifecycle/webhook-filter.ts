export function shouldDisableWebhookEvent(
  disabledEvents: string[],
  eventType: string,
  payload: { bucketId: string; name: string }
) {
  return (
    disabledEvents.includes(`Webhook:${eventType}`) ||
    disabledEvents.includes(`Webhook:${eventType}:${payload.bucketId}/${payload.name}`)
  )
}
