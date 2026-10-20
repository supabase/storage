import type { AcceptanceHttpClient } from './http'

export async function cleanupIcebergResources({
  client,
  token,
  bucketName,
  namespaceName,
  tableNames,
}: {
  client: AcceptanceHttpClient
  token: string
  bucketName: string
  namespaceName?: string
  tableNames: string[]
}) {
  const errors: unknown[] = []
  async function remove(route: string, successStatus: number) {
    try {
      await client.request('DELETE', route, { expectedStatus: [successStatus, 404], token })
    } catch (error) {
      errors.push(error)
    }
  }

  const bucket = encodeURIComponent(bucketName)
  if (namespaceName) {
    const namespace = `/iceberg/v1/${bucket}/namespaces/${encodeURIComponent(namespaceName)}`
    for (const table of [...tableNames].reverse()) {
      await remove(`${namespace}/tables/${encodeURIComponent(table)}?purgeRequested=true`, 204)
    }
    await remove(namespace, 204)
  }
  await remove(`/iceberg/bucket/${bucket}`, 200)

  if (errors.length > 0) {
    throw new AggregateError(errors, `Iceberg cleanup failed for bucket ${bucketName}`)
  }
}
