package com.salesforce.multicloudj.blob.driver;

/**
 * The versioning state of a bucket.
 *
 * <p>This is a substrate-agnostic model of bucket-level object versioning. Providers map their
 * native versioning representation onto these three states:
 *
 * <ul>
 *   <li>{@link #ENABLED} — versioning is active and new versions are retained on overwrite/delete.
 *   <li>{@link #SUSPENDED} — versioning was previously enabled but is currently paused. Existing
 *       versions are retained, but new overwrites do not create additional versions. Not every
 *       substrate distinguishes this state from {@link #UNVERSIONED}; a substrate that has no
 *       distinct suspended state never reports {@code SUSPENDED} from a get operation.
 *   <li>{@link #UNVERSIONED} — versioning has never been configured, or has been turned off.
 * </ul>
 */
public enum BucketVersioningStatus {
  /** Versioning is active; new versions are retained on overwrite and delete. */
  ENABLED,

  /** Versioning was previously enabled but is currently paused; existing versions are retained. */
  SUSPENDED,

  /** Versioning has never been configured, or has been turned off. */
  UNVERSIONED
}
