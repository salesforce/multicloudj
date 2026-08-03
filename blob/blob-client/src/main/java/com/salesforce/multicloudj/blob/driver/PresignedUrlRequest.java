package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.observability.OperationContext;
import java.time.Duration;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * This object contains the information necessary to generate Presigned URLs for upload/download
 * requests
 */
@Builder
@Getter
public class PresignedUrlRequest {

  /** The key/name of the blob to access */
  private final String key;

  /** The duration for which this presigned URL will be valid once it's created */
  private final Duration duration;

  /** The type of presignedUrl operation. Currently limited to upload/download */
  private final PresignedOperation type;

  /** Optional: Specify the metadata to be used in a presignedUrl upload */
  private final Map<String, String> metadata;

  /** Optional: Specify the tags to be used in a presignedUrl upload */
  private final Map<String, String> tags;

  /** Optional: Specify the KMS key ID to be used for encryption in a presignedUrl upload */
  private final String kmsKeyId;

  /**
   * Optional: Specify the Content-Disposition header to override in a presigned download URL.
   */
  private final String contentDisposition;

  /**
   * (Optional) The length of the content in bytes to be signed into the presigned upload URL.
   * When set, the substrate will reject uploads whose body size does not match.
   * A value of 0 means unset (no content-length constraint).
   */
  private final long contentLength;

  /**
   * (Optional) The content type to be signed into the presigned upload URL.
   * When set, the substrate will reject uploads whose Content-Type header does not match.
   */
  private final String contentType;

  /**
   * (Optional) The base64-encoded checksum value to be signed into the presigned upload URL.
   * When set, the substrate will reject uploads whose content hash does not match.
   */
  private final String checksumValue;

  /**
   * (Optional) The checksum algorithm used for the checksumValue.
   * Defaults to CRC32C when checksumValue is set but no algorithm is specified.
   *
   * <p><b>Ali OSS caveat:</b> The Ali provider maps {@code CRC32C} to its native
   * {@code x-oss-hash-crc64ecma} header. Callers targeting Ali must compute a CRC64-ECMA
   * hash (not a 4-byte CRC32C) and pass it as {@code checksumValue} with this field set
   * to {@code CRC32C}. A future {@code CRC64ECMA} enum value may be added to make this
   * explicit.
   */
  private final ChecksumMethod checksumAlgorithm;

  /**
   * (Optional) Per-call observability context carrying the correlation ID. The correlation ID is
   * never auto-generated; when it is null or missing it defaults to an empty string and tracing is
   * treated as disabled for log/trace correlation.
   */
  private final OperationContext operationContext;
}
