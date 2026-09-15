package com.salesforce.multicloudj.common.gcp;

public class GcpConstants {

  private GcpConstants() {}

  public static final String PROVIDER_ID = "gcp";

  /**
   * Reserved blob tag key that marks an object for lifecycle-based deletion. The tag value is the
   * number of days the object should live, measured from its creation time; a value that is absent,
   * blank, or not a positive integer leaves the object unmarked. The GCP blob implementation stamps
   * the object's custom time from this marker so a bucket lifecycle rule keyed off custom time can
   * delete the object; the deletion itself is performed by a bucket lifecycle rule configured
   * out-of-band.
   */
  public static final String LIFECYCLE_EXPIRATION_TAG_KEY = "expiration-days";
}
