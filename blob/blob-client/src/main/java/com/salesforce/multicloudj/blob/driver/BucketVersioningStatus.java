package com.salesforce.multicloudj.blob.driver;

/**
 * Represents the versioning configuration status of a bucket.
 *
 * <p>Bucket versioning can be in one of three states:
 *
 * <ul>
 *   <li>{@link #ENABLED} - Versioning is active; new object writes create version entries.
 *   <li>{@link #SUSPENDED} - Versioning was previously enabled but is now paused; existing versions
 *       are preserved but new writes do not create version entries.
 *   <li>{@link #UNSET} - Versioning has never been enabled on this bucket.
 * </ul>
 */
public enum BucketVersioningStatus {
  /** Versioning is active on the bucket. */
  ENABLED,

  /** Versioning was previously enabled but is now suspended. */
  SUSPENDED,

  /** Versioning has never been enabled on the bucket. */
  UNSET
}
