package com.salesforce.multicloudj.blob.driver;

/**
 * Supported checksum algorithms for upload integrity validation.
 */
public enum ChecksumMethod {
  CRC32C,
  SHA256
}
