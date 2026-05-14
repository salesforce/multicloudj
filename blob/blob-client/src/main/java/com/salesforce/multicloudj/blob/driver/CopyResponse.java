package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Wrapper object for copy result data */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class CopyResponse {
  private final String key;

  /**
   * The versionId of this blob. This value only serves a purpose for buckets with versioning
   * enabled, although non-versioned buckets may still return a value for it. Non-versioned buckets
   * should simply ignore the versionId value as it serves no purpose for them.
   */
  private final String versionId;

  private final String eTag;
  private final Instant lastModified;

  // BucketClient rebuilds this response to stamp the correlationId; excluding the field from
  // equals/hashCode keeps the rebuilt copy equal to the underlying provider response (e.g. for
  // mock-based tests), and excluding it from toString avoids leaking observability metadata.
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final String correlationId;
}
