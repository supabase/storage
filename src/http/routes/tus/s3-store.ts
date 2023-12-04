import { S3Store as BaseS3Store } from '@tus/s3-store'
import { Upload } from '@tus/server'
import { S3 } from '@aws-sdk/client-s3'

type MetadataValue = {
  file: Upload
  'upload-id': string
  'tus-version': string
}

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore - overwriting private getMetadata function for backwards compatibility
// TODO: remove this class after all tenants are migrated to the new tus server version
export class S3Store extends BaseS3Store {
  /**
   * Get the metadata for a file.
   * It keeps backwards compatibility from version 0.9 to 1.0.0
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

    console.log('getMetadata', id, bucket, cache)

    const { Metadata, Body } = await client.getObject({
      Bucket: bucket,
      Key: id + '.info',
    })

    if (Metadata?.file) {
      // OLD Implementation
      const file = JSON.parse(Metadata.file as string)
      cache.set(id, {
        'tus-version': Metadata?.['tus_version'] as string,
        'upload-id': Metadata?.['upload_id'] as string,
        file: new Upload({
          id,
          size: file.size ? Number.parseInt(file.size, 10) : undefined,
          offset: Number.parseInt(file.offset, 10),
          metadata: file.metadata,
          creation_date: Metadata?.['creation_date'] || undefined,
        }),
      })

      return cache.get(id) as MetadataValue
    }

    const file = JSON.parse((await Body?.transformToString()) as string)

    cache.set(id, {
      'tus-version': Metadata?.['tus-version'] as string,
      'upload-id': Metadata?.['upload-id'] as string,
      file: new Upload({
        id,
        size: file.size ? Number.parseInt(file.size, 10) : undefined,
        offset: Number.parseInt(file.offset, 10),
        metadata: file.metadata,
        creation_date: file.creation_date,
      }),
    })
    return cache.get(id) as MetadataValue
  }
}
