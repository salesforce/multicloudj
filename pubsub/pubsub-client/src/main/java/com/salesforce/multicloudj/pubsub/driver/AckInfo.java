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

  public Duration getVisibilityTimeout() {
    return visibilityTimeout;
  }
}
