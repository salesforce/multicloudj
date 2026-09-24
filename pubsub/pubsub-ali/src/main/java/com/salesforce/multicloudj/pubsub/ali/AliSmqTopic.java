package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.model.Base64TopicMessage;
import com.aliyun.mns.model.MessagePropertyValue;
import com.aliyun.mns.model.RawTopicMessage;
import com.aliyun.mns.model.TopicMessage;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Alibaba SMQ (MNS) topic publisher.
 *
 * <p>Publishes messages to an SMQ topic via {@code publishMessage}, which fans each message out to
 * the topic's subscriptions. Registered under the {@code alismqtopic} provider id.
 */
@AutoService(AbstractTopic.class)
public class AliSmqTopic extends AliBaseTopic<AliSmqTopic> {

  public static final String PROVIDER_ID = "alismqtopic";

  private final CloudTopic topic;

  public AliSmqTopic() {
    this(new Builder());
  }

  AliSmqTopic(Builder builder) {
    super(builder);
    this.topic = builder.topic;
  }

  @Override
  public String getProviderId() {
    return PROVIDER_ID;
  }

  /**
   * Converts a multicloudj {@link Message} into an SMQ SDK {@link TopicMessage}.
   *
   * <p>The body is placed on the wire per the configured {@code Base64EncodingStrategy}: a
   * base64-encoded body rides as a {@link Base64TopicMessage} (and the reserved base64 flag user
   * property is set so the receiver knows to decode it) while an XML-safe body rides as a
   * {@link RawTopicMessage}. Message metadata is mapped onto SMQ user properties by the shared
   * codec (see {@code buildUserProperties}), with the per-message limits enforced fail-fast here so
   * an over-limit message is rejected before publish. The reserved
   * {@link #RESERVED_TOPIC_ORIGINATED_KEY} marker is always stamped so the subscription can tell a
   * topic delivery from a direct message authoritatively.
   */
  protected TopicMessage toTopicMessage(Message message) {
    byte[] body = message.getBody() == null ? new byte[0] : message.getBody();
    boolean base64 = shouldBase64EncodeBody(body);
    TopicMessage topicMessage = base64 ? new Base64TopicMessage() : new RawTopicMessage();
    topicMessage.setMessageBody(body);
    Map<String, MessagePropertyValue> userProperties =
        buildUserProperties(message.getMetadata(), base64);
    if (userProperties == null) {
      // No metadata and no base64 flag, so buildUserProperties returned null; allocate a map to
      // hold just the marker.
      userProperties = new HashMap<>();
    }
    // Stamp the reserved topic-originated marker so the subscription can authoritatively identify a
    // topic delivery without sniffing the body shape. This is the only place the marker is set;
    // buildUserProperties already reserved its slot (see stampsTopicOriginatedMarker).
    userProperties.put(RESERVED_TOPIC_ORIGINATED_KEY, new MessagePropertyValue(true));
    topicMessage.setUserProperties(userProperties);
    return topicMessage;
  }

  @Override
  protected boolean stampsTopicOriginatedMarker() {
    return true;
  }

  /**
   * Publishes a batch of messages to the SMQ topic. SMQ has no topic batch API, so each message is
   * published with its own {@code publishMessage} call.
   *
   * <p>The entire batch is size-checked and converted to SMQ SDK messages up front, before any
   * {@code publishMessage} call, so a purely local failure — a message whose serialized size
   * exceeds the SMQ per-request limit (see {@code measureWireSize}), or metadata that exceeds the
   * SMQ per-message limits (see {@code toTopicMessage}) — fails fast without leaving an earlier
   * message already published.
   *
   * <p>A {@code publishMessage} failure part-way through the batch fails the entire batch, so every
   * message future in the batch completes exceptionally, including messages SMQ already published.
   * Under at-least-once delivery, a caller that retries the batch may therefore re-publish the
   * already-published messages, so consumers must tolerate duplicate delivery. The native SMQ
   * failure propagates to the public client, which maps it via {@code mapException}.
   */
  @Override
  protected void doSendBatch(List<Message> messages) {
    if (messages == null || messages.isEmpty()) {
      return;
    }
    // Size-check and convert the whole batch before publishing anything so a local failure (an
    // oversized message, or metadata over the SMQ per-message limits) surfaces before the first
    // publishMessage call, and no earlier message is published on a local error.
    List<TopicMessage> topicMessages = new ArrayList<>(messages.size());
    for (Message message : messages) {
      ensureWithinRequestSizeLimit(message);
      topicMessages.add(toTopicMessage(message));
    }
    for (TopicMessage topicMessage : topicMessages) {
      topic.publishMessage(topicMessage);
    }
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  /** Builder for {@link AliSmqTopic}. */
  public static class Builder extends AliBaseTopic.Builder<Builder, AliSmqTopic> {

    private CloudTopic topic;

    public Builder() {
      this.providerId = PROVIDER_ID;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public AliSmqTopic build() {
      if (topicName == null || topicName.trim().isEmpty()) {
        throw new InvalidArgumentException("Topic name cannot be null or empty");
      }
      if (smqClient == null) {
        smqClient = SmqClientFactory.buildSmqClient(endpoint, credentialsOverrider, proxyEndpoint);
      }
      topic = smqClient.getTopicRef(topicName);
      return new AliSmqTopic(this);
    }
  }
}
