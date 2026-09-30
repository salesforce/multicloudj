package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Bucket-level versioning configuration.
 *
 * <p>Returned by {@link BlobStore#getBucketVersioning()} to describe the bucket's current
 * versioning state.
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
}
