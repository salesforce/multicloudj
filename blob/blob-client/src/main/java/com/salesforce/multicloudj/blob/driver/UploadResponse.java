package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

/** Wrapper object for upload result data */
@Builder
@Value
public class UploadResponse {
  String key;

  /**
   * The versionId of this blob. This value only serves a purpose for buckets with versioning
   * enabled, although non-versioned buckets may still return a value for it. Non-versioned buckets
   * should simply ignore the versionId value as it serves no purpose for them.
   */
  String versionId;

  String eTag;

  /**
   * The base64-encoded checksum value returned by the service (if validation was requested). The
   * checksum algorithm is provider-specific: - AWS: CRC32C - GCP: CRC32C - Alibaba: CRC64 (not that
   * this is the disparity and will raise a request to alibaba to support crc32c)
   */
  String checksumValue;

  /**
   * The correlation ID associated with this upload. Echoes the application-supplied value when
   * provided via {@code OperationContext}, or the SDK-generated UUID when not. Excluded from
   * {@code equals}/{@code hashCode}/{@code toString} because it is observability metadata, not
   * part of the resource's identity.
   */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  String correlationId;
}
