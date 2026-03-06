package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/** Configuration for object lock (WORM protection) when uploading objects. */
@Builder
@Getter
public class ObjectLockConfiguration {

  /** Retention mode: GOVERNANCE (can be bypassed) or COMPLIANCE (cannot be bypassed). */
  private final RetentionMode mode;

  /** Date until which the object should be retained (WORM protection). */
  private final Instant retainUntilDate;

  /** Legal hold status: prevents deletion/modification until removed. */
  private final boolean legalHold;

  /**
   * Event-based hold: Resets the object's time-in-bucket for retention policy calculation when
   * released.
   *
   * <p>Temporary hold: Does not affect retention policy calculation.
   *
   * <p>Default: false (temporaryHold)
   */
  private final Boolean useEventBasedHold;
}
