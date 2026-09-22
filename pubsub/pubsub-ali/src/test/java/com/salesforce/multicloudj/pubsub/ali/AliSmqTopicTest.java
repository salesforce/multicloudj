package com.salesforce.multicloudj.pubsub.ali;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Base64TopicMessage;
import com.aliyun.mns.model.MessagePropertyValue;
import com.aliyun.mns.model.PropertyType;
import com.aliyun.mns.model.RawTopicMessage;
import com.aliyun.mns.model.TopicMessage;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AliSmqTopicTest {

  private final List<AutoCloseable> closeables = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable c : closeables) {
      c.close();
    }
    closeables.clear();
  }

  private AliSmqTopic topic(MNSClient client, CloudTopic cloudTopic) {
    return topic(client, cloudTopic, AliBaseTopic.Base64EncodingStrategy.AUTO);
  }

  private AliSmqTopic topic(
      MNSClient client, CloudTopic cloudTopic, AliBaseTopic.Base64EncodingStrategy strategy) {
    when(client.getTopicRef("test-topic")).thenReturn(cloudTopic);
    AliSmqTopic.Builder builder = new AliSmqTopic.Builder();
    builder.withSmqClient(client);
    builder.withTopicName("test-topic");
    builder.withBodyEncodingStrategy(strategy);
    AliSmqTopic topic = builder.build();
    closeables.add(topic);
    return topic;
  }

  @Test
  void getProviderIdIsAliSmqTopic() throws Exception {
    try (AliSmqTopic t = new AliSmqTopic()) {
      assertEquals("alismqtopic", t.getProviderId());
    }
  }

  @Test
  void doSendBatchPublishesEachMessageInOrderViaPublishMessage() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    List<Message> batch =
        List.of(
            Message.builder().withBody("a".getBytes(UTF_8)).build(),
            Message.builder().withBody("b".getBytes(UTF_8)).build(),
            Message.builder().withBody("c".getBytes(UTF_8)).build());
    topic.doSendBatch(batch);

    // SMQ has no topic batch API, so every message is published with its own publishMessage call,
    // in the order the batch supplied them.
    List<TopicMessage> published = capturePublished(cloudTopic, 3);
    assertArrayEquals("a".getBytes(UTF_8), publishedBody(published.get(0)));
    assertArrayEquals("b".getBytes(UTF_8), publishedBody(published.get(1)));
    assertArrayEquals("c".getBytes(UTF_8), publishedBody(published.get(2)));
  }

  @Test
  void alwaysStrategyPublishesBase64TopicMessageWithFlag() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic, AliBaseTopic.Base64EncodingStrategy.ALWAYS);

    topic.send(Message.builder().withBody("hello".getBytes(UTF_8)).build());

    TopicMessage published = captureSinglePublished(cloudTopic);
    // ALWAYS base64-encodes even a valid UTF-8 body: it rides as a Base64TopicMessage with the
    // reserved flag set, and decodes back to the original bytes.
    assertInstanceOf(Base64TopicMessage.class, published);
    assertTrue(hasBase64Flag(published));
    assertArrayEquals("hello".getBytes(UTF_8), publishedBody(published));
  }

  @Test
  void autoStrategyPublishesBase64TopicMessageWithFlagForNonXmlSafeBody() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic); // AUTO is the default

    byte[] nonUtf8 = {(byte) 0xFF, (byte) 0xFE, (byte) 0x80, 0x01};
    topic.send(Message.builder().withBody(nonUtf8).build());

    TopicMessage published = captureSinglePublished(cloudTopic);
    // A non-UTF-8 body cannot ride as XML text, so AUTO base64-encodes it and sets the flag.
    assertInstanceOf(Base64TopicMessage.class, published);
    assertTrue(hasBase64Flag(published));
    assertArrayEquals(nonUtf8, publishedBody(published));
  }

  @Test
  void autoStrategyPublishesRawTopicMessageWithoutFlagForXmlSafeBody() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic); // AUTO is the default

    byte[] xmlSafe = "a<b>c&d\t\né-Ω-😀".getBytes(UTF_8);
    topic.send(Message.builder().withBody(xmlSafe).build());

    TopicMessage published = captureSinglePublished(cloudTopic);
    // An XML-safe UTF-8 body rides raw with no flag.
    assertInstanceOf(RawTopicMessage.class, published);
    assertFalse(hasBase64Flag(published));
    assertArrayEquals(xmlSafe, publishedBody(published));
  }

  @Test
  void metadataRoundTripsThroughSharedCodecAsUserProperties() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    // A non-conforming key (the space is outside the SMQ attribute-name charset) exercises the
    // same shared metadata codec the queue publisher uses: it is hex-escaped on the wire and
    // round-trips.
    String rawKey = "trace.id key_1";
    topic.send(
        Message.builder().withBody("b".getBytes(UTF_8)).withMetadata(rawKey, "42").build());

    TopicMessage published = captureSinglePublished(cloudTopic);
    Map<String, MessagePropertyValue> props = published.getUserProperties();
    assertEquals(1, props.size());
    String encodedKey = props.keySet().iterator().next();
    assertEquals(AliBaseTopic.encodeMetadataKey(rawKey), encodedKey);
    assertEquals(rawKey, AliBaseTopic.decodeMetadataKey(encodedKey));
    MessagePropertyValue value = props.get(encodedKey);
    assertEquals(PropertyType.STRING, value.getDataType());
    assertEquals("42", value.getStringValueByType());
  }

  @Test
  void overLimitMetadataFailsFastBeforeAnyPublish() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    Message.Builder builder = Message.builder().withBody("b".getBytes(UTF_8));
    for (int i = 0; i <= AliBaseTopic.MAX_USER_PROPERTIES; i++) {
      builder.withMetadata("k" + i, "v");
    }
    Message message = builder.build();

    // Over-limit metadata is rejected during the up-front conversion, before any publishMessage.
    assertThrows(InvalidArgumentException.class, () -> topic.send(message));
    verify(cloudTopic, never()).publishMessage(any());
  }

  @Test
  void localConversionFailureInBatchPublishesNothing() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    // The whole batch is converted before any publish, so a local conversion failure on a later
    // message (metadata value over the SMQ per-value limit) fails fast without publishing the
    // earlier, valid message.
    String tooLong = "x".repeat(AliBaseTopic.MAX_PROPERTY_VALUE_LENGTH + 1);
    List<Message> batch =
        List.of(
            Message.builder().withBody("ok".getBytes(UTF_8)).build(),
            Message.builder().withBody("bad".getBytes(UTF_8)).withMetadata("k", tooLong).build());

    assertThrows(InvalidArgumentException.class, () -> topic.doSendBatch(batch));
    verify(cloudTopic, never()).publishMessage(any());
  }

  @Test
  void oversizedMessageInBatchFailsFastBeforeAnyPublish() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic); // AUTO keeps this XML-safe body raw

    // A ~70 KB XML-safe body measures over the 64 KB per-request limit on its own. Every message is
    // size-checked up front, so the oversized message fails fast before any publishMessage, and the
    // earlier valid message is never published.
    List<Message> batch =
        List.of(
            Message.builder().withBody("ok".getBytes(UTF_8)).build(),
            Message.builder().withBody(bytesOf(70_000)).build());

    assertThrows(InvalidArgumentException.class, () -> topic.doSendBatch(batch));
    verify(cloudTopic, never()).publishMessage(any());
  }

  @Test
  void doSendBatchPropagatesServiceExceptionForClientToMap() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    // A ServiceException from publishMessage is not mapped inside doSendBatch: it propagates raw so
    // the public pubsub client maps it via mapException, the boundary every provider relies on. The
    // error-code translation itself is covered by SmqExceptionMapperTest.
    ServiceException denied = mock(ServiceException.class);
    when(cloudTopic.publishMessage(any())).thenThrow(denied);

    List<Message> batch = List.of(Message.builder().withBody("x".getBytes(UTF_8)).build());
    ServiceException thrown = assertThrows(ServiceException.class, () -> topic.doSendBatch(batch));
    assertSame(denied, thrown);
  }

  @Test
  void emptyOrNullBatchPublishesNothing() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topic(client, cloudTopic);

    topic.doSendBatch(List.of());
    topic.doSendBatch(null);

    verify(cloudTopic, never()).publishMessage(any());
  }

  @Test
  void builderRequiresTopicName() {
    MNSClient client = mock(MNSClient.class);
    AliSmqTopic.Builder builder = new AliSmqTopic.Builder();
    builder.withSmqClient(client);
    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void closeClosesSmqClientOnNormalPath() throws Exception {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    AliSmqTopic topic = topicWithClient(client, cloudTopic);

    topic.close();

    verify(client).close();
  }

  @Test
  void closeSurfacesSmqClientCloseFailureWhenShutdownSucceeds() {
    MNSClient client = mock(MNSClient.class);
    CloudTopic cloudTopic = mock(CloudTopic.class);
    RuntimeException clientCloseError = new RuntimeException("client close failed");
    doThrow(clientCloseError).when(client).close();
    AliSmqTopic topic = topicWithClient(client, cloudTopic);

    // When shutdown succeeds, a client-close failure is not swallowed: it propagates directly.
    RuntimeException thrown = assertThrows(RuntimeException.class, topic::close);
    assertSame(clientCloseError, thrown);
  }

  private static AliSmqTopic topicWithClient(MNSClient client, CloudTopic cloudTopic) {
    when(client.getTopicRef("test-topic")).thenReturn(cloudTopic);
    AliSmqTopic.Builder builder = new AliSmqTopic.Builder();
    builder.withSmqClient(client);
    builder.withTopicName("test-topic");
    return builder.build();
  }

  /** Captures the single SMQ topic message published through the one publishMessage call. */
  private static TopicMessage captureSinglePublished(CloudTopic cloudTopic) {
    return capturePublished(cloudTopic, 1).get(0);
  }

  /** Captures the {@code count} SMQ topic messages published, verifying exactly that many calls. */
  private static List<TopicMessage> capturePublished(CloudTopic cloudTopic, int count) {
    ArgumentCaptor<TopicMessage> captor = ArgumentCaptor.forClass(TopicMessage.class);
    verify(cloudTopic, times(count)).publishMessage(captor.capture());
    return captor.getAllValues();
  }

  /** True if the published topic message carries the reserved base64 flag set to true. */
  private static boolean hasBase64Flag(TopicMessage published) {
    Map<String, MessagePropertyValue> props = published.getUserProperties();
    if (props == null) {
      return false;
    }
    MessagePropertyValue flag = props.get(AliBaseTopic.RESERVED_BASE64_FLAG_KEY);
    return flag != null && "true".equalsIgnoreCase(flag.getStringValueByType());
  }

  /**
   * Recovers the original body bytes from a published topic message. A {@link RawTopicMessage}
   * stores the raw bytes directly, so {@code getMessageBodyAsBytes()} returns them as-is; a
   * {@link Base64TopicMessage} stores the body base64-encoded, so {@code getMessageBodyAsBytes()}
   * returns the base64 bytes that decode back to the original.
   */
  private static byte[] publishedBody(TopicMessage published) {
    if (published instanceof Base64TopicMessage) {
      return Base64.getDecoder().decode(published.getMessageBodyAsBytes());
    }
    return published.getMessageBodyAsBytes();
  }

  /** An XML-safe ASCII body of the given length, so AUTO keeps it raw and its wire size ~= len. */
  private static byte[] bytesOf(int length) {
    byte[] body = new byte[length];
    for (int i = 0; i < length; i++) {
      body[i] = (byte) ('a' + (i % 26));
    }
    return body;
  }
}
