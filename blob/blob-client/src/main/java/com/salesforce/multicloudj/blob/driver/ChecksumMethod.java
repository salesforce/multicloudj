package com.salesforce.multicloudj.blob.driver;

/**
 * Supported checksum algorithms for upload integrity validation.
 *
 * <p>Not every algorithm is supported by every substrate. Providers reject algorithms their
 * cloud SDK cannot honor (for example, OSS exposes only CRC64-ECMA for object checksums, while
 * S3/GCS expose CRC32C). The substrate-native default for a checksum-enabled request with no
 * explicit algorithm is therefore resolved per provider.
 */
public enum ChecksumMethod {
  CRC32C,
  SHA256,
  CRC64
}
