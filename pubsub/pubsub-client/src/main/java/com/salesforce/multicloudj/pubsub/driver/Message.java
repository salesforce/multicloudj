package com.salesforce.multicloudj.pubsub.driver;

import java.util.Map;
import java.util.function.Function;

/**
 * Message represents a message in the pubsub system.
 *
 * <p>Messages contain the actual data being transmitted along with metadata and acknowledgment
 * information when received from subscriptions.
 */
public class Message {

  private String loggableID;
  private byte[] body;
  private Map<String, String> metadata;
  private Map<String, String> systemMetadata;
  private AckID ackID;
  private Function<Object, Boolean> asFunc;
  private Function<Function<Object, Boolean>, Exception> beforeSend;
  private Function<Function<Object, Boolean>, Exception> afterSend;

  // Private constructor - use builder
  private Message() {}

  /**
   * Gets the loggable ID for this message (useful for debugging). This is set for received
   * messages.
   *
   * @return The loggable ID, or null if not set
   */
  public String getLoggableID() {
    return loggableID;
  }

  /**
   * Gets the message body content.
   *
   * @return The message body as a byte array
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * Gets the message metadata.
   *
   * @return A map of metadata key-value pairs, or null if no metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Gets the read-only system metadata attached to a received message. See {@link
   * SystemMetadataKeys} for the well-known keys.
   *
   * @return A map of system metadata key-value pairs, or null if no system metadata
   */
  public Map<String, String> getSystemMetadata() {
    return systemMetadata;
  }

  /**
   * Gets the acknowledgment ID for this message. This is only set for received messages.
   *
   * @return The ack ID, or null if not set
   */
  public AckID getAckID() {
    return ackID;
  }

  /**
   * Converts the message to a driver-specific type.
   *
   * @param target The target type to convert to
   * @return true if the conversion was successful, false otherwise
   */
  public boolean as(Object target) {
    return asFunc != null && asFunc.apply(target);
  }

  /**
   * Well-known keys of the system metadata exposed by {@link Message#getSystemMetadata()}.
   *
   * <p>System metadata is read-only on received messages and is ignored when a message is sent. An
   * absent key means the value is not available for that delivery.
   */
  public static final class SystemMetadataKeys {

    private SystemMetadataKeys() {}

    /**
     * Number of times the message has been delivered, including the current delivery. The value is
     * a base-10 positive integer string, 1-based (the first delivery is "1"), and approximate.
     */
    public static final String DELIVERY_COUNTER = "deliveryCounter";
  }

  /** Builder for creating Message instances. */
  public static class Builder {
    private final Message message = new Message();

    /**
     * Sets the message body.
     *
     * @param body The message body as bytes
     * @return This builder instance
     */
    public Builder withBody(byte[] body) {
      message.body = body;
      return this;
    }

    /**
     * Sets the message body from a string.
     *
     * @param body The message body as a string (will be UTF-8 encoded)
     * @return This builder instance
     */
    public Builder withBody(String body) {
      message.body = body.getBytes();
      return this;
    }

    /**
     * Sets the message metadata.
     *
     * @param metadata A map of metadata key-value pairs
     * @return This builder instance
     */
    public Builder withMetadata(Map<String, String> metadata) {
      message.metadata = metadata;
      return this;
    }

    /**
     * Adds a single metadata key-value pair.
     *
     * @param key The metadata key
     * @param value The metadata value
     * @return This builder instance
     */
    public Builder withMetadata(String key, String value) {
      if (message.metadata == null) {
        message.metadata = new java.util.HashMap<>();
      }
      message.metadata.put(key, value);
      return this;
    }

    /**
     * Sets the system metadata (typically used internally by drivers).
     *
     * @param systemMetadata A map of system metadata key-value pairs
     * @return This builder instance
     */
    public Builder withSystemMetadata(Map<String, String> systemMetadata) {
      message.systemMetadata = systemMetadata;
      return this;
    }

    /**
     * Sets the loggable ID (typically used internally by drivers).
     *
     * @param loggableID The loggable ID
     * @return This builder instance
     */
    public Builder withLoggableID(String loggableID) {
      message.loggableID = loggableID;
      return this;
    }

    /**
     * Sets the acknowledgment ID (typically used internally by drivers).
     *
     * @param ackID The acknowledgment ID
     * @return This builder instance
     */
    public Builder withAckID(AckID ackID) {
      message.ackID = ackID;
      return this;
    }

    /**
     * Sets the as function for driver-specific type conversion.
     *
     * @param asFunc The as function
     * @return This builder instance
     */
    public Builder withAsFunc(Function<Object, Boolean> asFunc) {
      message.asFunc = asFunc;
      return this;
    }

    /**
     * Sets the before send callback.
     *
     * @param beforeSend The before send callback
     * @return This builder instance
     */
    public Builder withBeforeSend(Function<Function<Object, Boolean>, Exception> beforeSend) {
      message.beforeSend = beforeSend;
      return this;
    }

    /**
     * Sets the after send callback.
     *
     * @param afterSend The after send callback
     * @return This builder instance
     */
    public Builder withAfterSend(Function<Function<Object, Boolean>, Exception> afterSend) {
      message.afterSend = afterSend;
      return this;
    }

    /**
     * Builds and returns a new Message instance.
     *
     * @return A new Message
     */
    public Message build() {
      return message;
    }
  }

  /**
   * Creates a new message builder.
   *
   * @return A new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
