package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Bucket-level versioning configuration.
 *
 * <p>Used both as the result of {@link BlobStore#getBucketVersioning()} (describing the bucket's
 * current versioning state) and as the desired target passed to {@link
 * BlobStore#setBucketVersioning(BucketVersioningConfiguration)}.
 */
@Builder
@Getter
public class BucketVersioningConfiguration {

  /** The bucket's versioning state. */
  private final BucketVersioningStatus status;

  /**
   * Convenience factory for a configuration with the given status.
   *
   * @param status the versioning status
   * @return a configuration carrying {@code status}
   */
  public static BucketVersioningConfiguration of(BucketVersioningStatus status) {
    return BucketVersioningConfiguration.builder().status(status).build();
  }

  /**
   * Whether versioning is currently enabled.
   *
   * @return {@code true} if {@link #getStatus()} is {@link BucketVersioningStatus#ENABLED}
   */
  public boolean isEnabled() {
    return status == BucketVersioningStatus.ENABLED;
  }
}
