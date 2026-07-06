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
  /**
   * MD5, sent as the RFC 1864 {@code Content-MD5} header. Validated as a caller-supplied digest
   * on single-object and presigned-URL uploads by every substrate this SDK supports. Not used for
   * multipart uploads (per-part {@code Content-MD5} is not available on every substrate's
   * multipart API).
   */
  MD5
}
