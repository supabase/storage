import { MetadataValue, S3Store as BaseS3Store } from '@tus/s3-store'
import { Upload } from '@tus/server'
import { S3 } from '@aws-sdk/client-s3'

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore - overwriting private getMetadata function for backwards compatibility
export class S3Store extends BaseS3Store {
  /**
   * Get the metadata for a file.
   * It keeps backwards compatibility from version 0.9 to 1.0.0
   * TODO: remove this after all tenants are migrated to the new tus server version
   * @param id
   */
  private async getMetadata(id: string): Promise<MetadataValue> {
    /* eslint-disable @typescript-eslint/ban-ts-comment */
    // @ts-ignore private property
    const cache = this.cache as Map<string, MetadataValue>
    // @ts-ignore private property
    const bucket = this.bucket as string
    // @ts-ignore private property
    const client = this.client as S3
    /* eslint-enable */

    const cached = cache.get(id)
    if (cached?.file) {
      return cached
    }

    const { Metadata, Body } = await client.getObject({
      Bucket: bucket,
      Key: id + '.info',
    })

    if (Metadata?.file) {
      // OLD Implementation
      // TODO: remove this after all tenants are migrated to the new tus server version
      const file = JSON.parse(Metadata.file as string)
      const metadata: MetadataValue = {
        'tus-version': Metadata?.['tus_version'] as string,
        'upload-id': Metadata?.['upload_id'] as string,
        file: new Upload({
          id,
          size: file.size ? Number.parseInt(file.size, 10) : undefined,
          offset: Number.parseInt(file.offset, 10),
          metadata: file.metadata,
          creation_date: Metadata?.['creation_date'] || undefined,
        }),
      }
      cache.set(id, metadata)

      return metadata
    }

    const file = JSON.parse((await Body?.transformToString()) as string)
    const metadata: MetadataValue = {
      'tus-version': Metadata?.['tus-version'] as string,
      'upload-id': Metadata?.['upload-id'] as string,
      file: new Upload({
        id,
        size: file.size ? Number.parseInt(file.size, 10) : undefined,
        offset: Number.parseInt(file.offset, 10),
        metadata: file.metadata,
        creation_date: file.creation_date,
      }),
    }

    cache.set(id, metadata)
    return metadata
  }
}
