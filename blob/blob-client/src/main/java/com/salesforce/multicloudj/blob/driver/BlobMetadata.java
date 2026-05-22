package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** Blob metadata data object */
@Builder
@Getter
public class BlobMetadata {

  private final String key;

  /**
   * The versionId of this blob. This value only serves a purpose for buckets with versioning
   * enabled, although non-versioned buckets may still return a value for it. Non-versioned buckets
   * should simply ignore the versionId value as it serves no purpose for them.
   */
  private final String versionId;

  private final String eTag;
  private final long objectSize;

  @Singular("metadata")
  private final Map<String, String> metadata;

  private final Instant lastModified;

  /** The creation time of the blob. This represents when the blob was originally created. */
  private final Instant createdTime;

  private final byte[] md5;

  /**
   * Base64-encoded CRC32C checksum of the object as reported by the substrate, or {@code null} if
   * the substrate did not surface one. Populated by GCS for every object (including composite
   * objects, which have no MD5) and by AWS when the {@code GetObject} request was issued with
   * checksum validation enabled (see {@link DownloadRequest.Builder#withChecksumValidation}).
   */
  private final String crc32c;

  /**
   * Base64-encoded SHA-256 checksum of the object as reported by the substrate, or {@code null} if
   * the substrate did not surface one. On AWS this is populated only when the object was uploaded
   * with {@code SHA256} as the flexible-checksum algorithm and {@code GetObject} was issued with
   * checksum validation enabled.
   */
  private final String sha256;

  /**
   * Base64-encoded CRC32 checksum of the object as reported by the substrate, or {@code null} if
   * not surfaced.
   */
  private final String crc32;

  /**
   * Base64-encoded SHA-1 checksum of the object as reported by the substrate, or {@code null} if
   * not surfaced.
   */
  private final String sha1;

  /** The content type of the blob (e.g., "application/octet-stream", "application/x-directory") */
  private final String contentType;

  /** Object lock information for this blob. null if object lock is not configured. */
  private final ObjectLockInfo objectLockInfo;

  /** The correlation ID associated with the operation that produced this metadata. */
  private final String correlationId;
}
