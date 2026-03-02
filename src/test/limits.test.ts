import { isValidBucketName } from '@storage/limits'

describe('isValidBucketName', () => {
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names
  test('bucket name follows s3 compatibility', () => {
    const validBucketNames = [
      'my', // Bucket names must be between 3 (min) and 63 (max) characters long.
      'a'.repeat(64), // Bucket names must be between 3 (min) and 63 (max) characters long.
      'my_bucket', // Bucket names can consist only of lowercase letters, numbers, periods (.), and hyphens (-).
      'my?bucket', // Bucket names can consist only of lowercase letters, numbers, periods (.), and hyphens (-).
      'my bucket', // Bucket names can consist only of lowercase letters, numbers, periods (.), and hyphens (-).
      '_my', // Bucket names must begin and end with a letter or number.
      'my..bucket', // Bucket names must not contain two adjacent periods.
      '192.168.5.4', // Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
      'xn--mybucket', // Bucket names must not start with the prefix xn--.
      'sthree-mybucket', // Bucket names must not start with the prefix sthree-.
      'amzn-s3-demo-mybucket', // Bucket names must not start with the prefix amzn-s3-demo-.
      'mybucket-s3alias', // Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias names. For more information, see Access point aliases.
      '--ol-s3', // Bucket names must not end with the suffix --ol-s3. This suffix is reserved for Object Lambda Access Point alias names. For more information, see How to use a bucket-style alias for your S3 bucket Object Lambda Access Point.
      '.mrap', // Bucket names must not end with the suffix .mrap. This suffix is reserved for Multi-Region Access Point names. For more information, see Rules for naming Amazon S3 Multi-Region Access Points.
      '--x-s3', // Bucket names must not end with the suffix --x-s3. This suffix is reserved for directory buckets. For more information, see Directory bucket naming rules.
      '--table-s3', // Bucket names must not end with the suffix --table-s3. This suffix is reserved for S3 Tables buckets. For more information, see Amazon S3 table bucket, table, and namespace naming rules.
      'my.bucket', // Buckets used with Amazon S3 Transfer Acceleration can't have periods (.) in their names. For more information about Transfer Acceleration, see Configuring fast, secure file transfers using Amazon S3 Transfer Acceleration.
    ]
    validBucketNames.forEach((name) => {
      expect(isValidBucketName(name)).toBe(true) // must be false
    })
  })
})
