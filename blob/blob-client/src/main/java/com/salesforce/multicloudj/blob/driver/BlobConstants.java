package com.salesforce.multicloudj.blob.driver;

/**
 * Internal constants shared across the blob provider implementations.
 *
 * <p><strong>Not part of the public API.</strong> These constants are an implementation detail
 * shared between the driver and the provider modules (and the conformance tests). They are not
 * intended for end-user code and may change without notice.
 */
public final class BlobConstants {

  private BlobConstants() {}

  /**
   * Reserved tag key that marks an object for lifecycle-based deletion. The tag value is the number
   * of days the object should live, measured from its creation time; a value that is absent, blank,
   * or not a positive integer leaves the object unmarked. The actual deletion is performed by a
   * bucket lifecycle rule configured out-of-band. See the blob store guide for the behavior and the
   * bucket rule each substrate requires.
   */
  public static final String LIFECYCLE_EXPIRATION_TAG_KEY = "expiration-days";
}
