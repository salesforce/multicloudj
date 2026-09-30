package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.observability.OperationContext;
import lombok.Builder;
import lombok.Getter;

/** POJO for copy requests. */
@Builder
@Getter
public class CopyRequest {

  /** The name of the destination bucket */
  private final String destBucket;

  /** The key of the blob to copy */
  private final String srcKey;

  /**
   * This field is optional. It's only used if you're copying from a bucket that has versioning
   * enabled. Explicitly specifying a versionId is only useful if you want to target the non-latest
   * version of a blob. This value should be null for non-versioned buckets, or if you want the
   * latest version of a versioned blob.
   */
  private final String srcVersionId;

  /** The key of the blob you're copying into in the destination bucket */
  private final String destKey;

  /**
   * (Optional) Per-call observability context carrying the correlation ID. The correlation ID is
   * never auto-generated; when it is null or missing it defaults to an empty string and tracing is
   * treated as disabled. When supplied, it is echoed back via the response.
   */
  private final OperationContext operationContext;
}
