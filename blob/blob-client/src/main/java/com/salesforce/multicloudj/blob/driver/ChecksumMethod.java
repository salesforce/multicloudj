package com.salesforce.multicloudj.blob.driver;

/**
 * Supported checksum algorithms for upload integrity validation.
 *
 * <p>Support varies by substrate: a provider rejects a caller-supplied algorithm its cloud SDK
 * cannot honor. For a checksum-enabled request with no explicit algorithm, the substrate-native
 * default is resolved per provider.
 */
public enum ChecksumMethod {
  CRC32C,
  SHA256,
  CRC64,
  MD5
}
