package com.salesforce.multicloudj.pubsub.driver;

import java.time.Duration;

/**
 * A single ack or nack operation on a message.
 *
 * <ul>
 *   <li>{@code ackID} — which message.
 *   <li>{@code isAck} — {@code true} for ack, {@code false} for nack.
 *   <li>{@code visibilityTimeout} — nack-only override; {@code null} means use the subscription
 *       default.
 * </ul>
 */
public class AckInfo {
  private final AckID ackID;
  private final boolean isAck;
  private final Duration visibilityTimeout;

  public AckInfo(AckID ackID, boolean isAck) {
    this(ackID, isAck, null);
  }

  public AckInfo(AckID ackID, boolean isAck, Duration visibilityTimeout) {
    this.ackID = ackID;
    this.isAck = isAck;
    this.visibilityTimeout = visibilityTimeout;
  }

  public AckID getAckID() {
    return ackID;
  }

  public boolean isAck() {
    return isAck;
  }

  /**
   * Per-nack visibility timeout, or {@code null} to use the subscription default. Only relevant on
   * the nack path.
   */
  public Duration getVisibilityTimeout() {
    return visibilityTimeout;
  }
}
