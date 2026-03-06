package com.salesforce.multicloudj.blob.driver;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/**
 * Object lock information for a blob (retrieved from metadata).
 *
 * <p>This represents the current object lock state of an object, including:
 *
 * <ul>
 *   <li>Retention expiration date
 *   <li>Legal hold status
 * </ul>
 */
@Builder
@Getter
public class ObjectLockInfo {

  /** Retention mode: GOVERNANCE or COMPLIANCE. */
  private final RetentionMode mode;

  /** Date until which the object is retained. */
  private final Instant retainUntilDate;

  /** Whether legal hold is currently applied. */
  private final boolean legalHold;

  private final Boolean useEventBasedHold;
}
