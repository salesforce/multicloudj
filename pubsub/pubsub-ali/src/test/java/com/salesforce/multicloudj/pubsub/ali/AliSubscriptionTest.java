package com.salesforce.multicloudj.pubsub.ali;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.BatchDeleteException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.aliyun.mns.model.ErrorMessageResult;
import com.aliyun.mns.model.Message.MessageBodyType;
import com.aliyun.mns.model.MessagePropertyValue;
import com.aliyun.mns.model.PropertyType;
import com.aliyun.mns.model.serialize.queue.MessageDeserializer;
import com.aliyun.mns.model.serialize.queue.MessageSerializer;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.AckInfo;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AliSubscriptionTest {

  private final List<AutoCloseable> closeables = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable c : closeables) {
      c.close();
    }
    closeables.clear();
  }

  private AliSubscription subscription(CloudQueue queue) {
    return subscription(queue, null);
  }

  private AliSubscription subscription(CloudQueue queue, Duration nackTimeout) {
    MNSClient client = mock(MNSClient.class);
    when(client.getQueueRef("test-queue")).thenReturn(queue);
    AliSubscription.Builder builder = new AliSubscription.Builder();
    builder.withSmqClient(client);
    builder.withSubscriptionName("test-queue");
    if (nackTimeout != null) {
      builder.withNackVisibilityTimeout(nackTimeout);
    }
    AliSubscription sub = builder.build();
    closeables.add(sub);
    return sub;
  }

  @Test
  void getProviderIdIsAli() throws Exception {
    try (AliSubscription s = new AliSubscription()) {
      assertEquals("ali", s.getProviderId());
    }
  }

  @Test
  void doReceiveBatchDecodesBodyAckIdAndLoggableId() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    com.aliyun.mns.model.Message raw = new com.aliyun.mns.model.Message();
    raw.setMessageBody("hello".getBytes(UTF_8), MessageBodyType.BASE64);
    raw.setReceiptHandle("rh-1");
    raw.setMessageId("mid-1");
    when(queue.batchPopMessage(anyInt())).thenReturn(List.of(raw));

    AliSubscription sub = subscription(queue);
    List<Message> received = sub.doReceiveBatch(10);

    assertEquals(1, received.size());
    assertArrayEquals("hello".getBytes(UTF_8), received.get(0).getBody());
    assertEquals("rh-1", received.get(0).getAckID().toString());
    assertEquals("mid-1", received.get(0).getLoggableID());
  }

  @Test
  void doReceiveBatchDecodesUserPropertiesIntoMetadata() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    com.aliyun.mns.model.Message source = new com.aliyun.mns.model.Message();
    source.setMessageBody("hi".getBytes(UTF_8), MessageBodyType.RAW_STRING);
    Map<String, MessagePropertyValue> props = new HashMap<>();
    // The key rides the wire hex-escaped; the value is a STRING.
    props.put(
        AliBaseTopic.encodeMetadataKey("trace.id"),
        new MessagePropertyValue(PropertyType.STRING, "abc"));
    // An empty value must serialize to <Value/> and survive a real deserialize present-and-empty.
    props.put("plain", new MessagePropertyValue(PropertyType.STRING, ""));
    source.setUserProperties(props);

    // Round-trip the message through the SMQ SDK's real XML serialize -> deserialize so the empty
    // value is exercised as the actual <Value/> wire form rather than an in-memory property object;
    // the deserializer supplies no receipt handle or id (the send-format serializer omits them), so
    // set them afterwards for the receive path.
    com.aliyun.mns.model.Message raw = serializeThenDeserialize(source);
    raw.setReceiptHandle("rh-1");
    raw.setMessageId("mid-1");
    when(queue.batchPopMessage(anyInt())).thenReturn(List.of(raw));

    AliSubscription sub = subscription(queue);
    List<Message> received = sub.doReceiveBatch(10);

    assertEquals(1, received.size());
    Map<String, String> metadata = received.get(0).getMetadata();
    assertEquals(2, metadata.size());
    assertEquals("abc", metadata.get("trace.id"));
    // The empty metadata value comes back present-and-empty after the real wire round-trip.
    assertEquals("", metadata.get("plain"));
  }

  @Test
  void doReceiveBatchWithoutUserPropertiesHasNullMetadata() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    com.aliyun.mns.model.Message raw = new com.aliyun.mns.model.Message();
    raw.setMessageBody("hi".getBytes(UTF_8), MessageBodyType.BASE64);
    raw.setReceiptHandle("rh-1");
    raw.setMessageId("mid-1");
    // No user properties set: the received message carries no metadata (left null, not empty).
    when(queue.batchPopMessage(anyInt())).thenReturn(List.of(raw));

    AliSubscription sub = subscription(queue);
    List<Message> received = sub.doReceiveBatch(10);

    assertNull(received.get(0).getMetadata());
  }

  @Test
  void doReceiveBatchTreatsEmptyQueueAsNoMessages() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    ServiceException empty = mock(ServiceException.class);
    when(queue.batchPopMessage(anyInt())).thenThrow(empty);
    when(queue.isMessageNotExist(empty)).thenReturn(true);

    AliSubscription sub = subscription(queue);
    assertTrue(sub.doReceiveBatch(10).isEmpty());
  }

  @Test
  void doReceiveBatchTreatsNullReturnAsNoMessages() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    // SMQ returns null (rather than throwing) when the short-poll receive finds no messages.
    when(queue.batchPopMessage(anyInt())).thenReturn(null);

    AliSubscription sub = subscription(queue);
    assertTrue(sub.doReceiveBatch(10).isEmpty());
  }

  @Test
  void doReceiveBatchMapsServiceError() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    ServiceException denied = mock(ServiceException.class);
    when(denied.getErrorCode()).thenReturn("AccessDenied");
    when(queue.batchPopMessage(anyInt())).thenThrow(denied);
    when(queue.isMessageNotExist(denied)).thenReturn(false);

    AliSubscription sub = subscription(queue);
    assertThrows(UnAuthorizedException.class, () -> sub.doReceiveBatch(10));
  }

  @Test
  void doSendAcksDeletesReceiptHandles() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);

    sub.doSendAcks(
        List.of(new AliSubscription.AliAckID("rh-1"), new AliSubscription.AliAckID("rh-2")));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
    verify(queue).batchDeleteMessage(captor.capture());
    assertEquals(List.of("rh-1", "rh-2"), captor.getValue());
  }

  @Test
  void doSendAcksSurfacesNonBenignServiceHandling() throws Exception {
    // A ServiceHandlingRequiredException with no wrapped batch failure and a non-benign code is
    // surfaced via the single-code fallback.
    CloudQueue queue = mock(CloudQueue.class);
    ServiceHandlingRequiredException e =
        new ServiceHandlingRequiredException(
            "denied", new RuntimeException("boom"), "AccessDenied", "req-1", "host-1");
    doThrow(e).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertThrows(
        UnAuthorizedException.class,
        () -> sub.doSendAcks(List.of(new AliSubscription.AliAckID("rh-1"))));
  }

  @Test
  void doSendAcksToleratesAlreadyGoneServiceHandling() throws Exception {
    // A ServiceHandlingRequiredException with no wrapped batch failure and a benign already-gone
    // code is tolerated via the single-code fallback.
    CloudQueue queue = mock(CloudQueue.class);
    ServiceHandlingRequiredException e =
        new ServiceHandlingRequiredException(
            "gone", new RuntimeException("boom"), "MessageNotExist", "req-1", "host-1");
    doThrow(e).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertDoesNotThrow(() -> sub.doSendAcks(List.of(new AliSubscription.AliAckID("rh-1"))));
  }

  @Test
  void doSendAcksToleratesAllBenignBatchDeleteFailures() throws Exception {
    // Real SDK shape: batchDeleteMessage wraps the per-handle BatchDeleteException in a
    // ServiceHandlingRequiredException. An all-benign failure map is tolerated.
    CloudQueue queue = mock(CloudQueue.class);
    Map<String, ErrorMessageResult> failures = new HashMap<>();
    failures.put("rh-1", errorResult("MessageNotExist"));
    failures.put("rh-2", errorResult("ReceiptHandleError"));
    doThrow(wrapBatchDelete(failures, "MessageNotExist")).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertDoesNotThrow(
        () ->
            sub.doSendAcks(
                List.of(
                    new AliSubscription.AliAckID("rh-1"),
                    new AliSubscription.AliAckID("rh-2"))));
  }

  @Test
  void doSendAcksSurfacesNonBenignInMixedBatchDeleteFailures() throws Exception {
    // Fail-open guard: the wrapper's own errorCode is the BENIGN entry, so a check that trusted
    // only that single code would silently swallow the non-benign per-handle failure. The code
    // must scan every entry and surface the non-benign AccessDenied as an UnAuthorizedException.
    CloudQueue queue = mock(CloudQueue.class);
    Map<String, ErrorMessageResult> failures = new HashMap<>();
    failures.put("rh-1", errorResult("MessageNotExist"));
    failures.put("rh-2", errorResult("AccessDenied"));
    doThrow(wrapBatchDelete(failures, "MessageNotExist")).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertThrows(
        UnAuthorizedException.class,
        () ->
            sub.doSendAcks(
                List.of(
                    new AliSubscription.AliAckID("rh-1"),
                    new AliSubscription.AliAckID("rh-2"))));
  }

  @Test
  void doSendAcksFailsClosedOnEmptyBatchDeleteFailures() throws Exception {
    // Fail closed: a wrapped batch failure carrying no per-handle detail must be surfaced, never
    // silently swallowed. An empty map yields an empty wrapper code, mapped to UnknownException.
    CloudQueue queue = mock(CloudQueue.class);
    doThrow(wrapBatchDelete(new HashMap<>(), "")).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertThrows(
        UnknownException.class,
        () -> sub.doSendAcks(List.of(new AliSubscription.AliAckID("rh-1"))));
  }

  @Test
  void doSendAcksSurfacesDirectBatchDeleteException() throws Exception {
    // Defensive branch: if a BatchDeleteException is ever thrown directly, its map is scanned the
    // same way and the non-benign entry is surfaced.
    CloudQueue queue = mock(CloudQueue.class);
    Map<String, ErrorMessageResult> failures = new HashMap<>();
    failures.put("rh-1", errorResult("MessageNotExist"));
    failures.put("rh-2", errorResult("QueueNotExist"));
    doThrow(new BatchDeleteException(failures)).when(queue).batchDeleteMessage(any());

    AliSubscription sub = subscription(queue);
    assertThrows(
        ResourceNotFoundException.class,
        () ->
            sub.doSendAcks(
                List.of(
                    new AliSubscription.AliAckID("rh-1"),
                    new AliSubscription.AliAckID("rh-2"))));
  }

  private static ServiceHandlingRequiredException wrapBatchDelete(
      Map<String, ErrorMessageResult> failures, String wrapperErrorCode) {
    // Mirrors CloudQueue.batchDeleteMessage: it catches the per-handle BatchDeleteException and
    // re-throws it as a ServiceHandlingRequiredException whose cause is that batch exception.
    BatchDeleteException cause = new BatchDeleteException(failures);
    return new ServiceHandlingRequiredException(
        cause.getMessage(), cause, wrapperErrorCode, "req-1", "host-1");
  }

  private static ErrorMessageResult errorResult(String code) {
    ErrorMessageResult result = new ErrorMessageResult();
    result.setErrorCode(code);
    return result;
  }

  @Test
  void doSendNacksUsesPerMessageVisibilityTimeout() {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);

    AckInfo nack = new AckInfo(new AliSubscription.AliAckID("rh-1"), false, Duration.ofSeconds(30));
    sub.doSendNacks(List.of(nack));

    verify(queue).changeMessageVisibility("rh-1", 30);
  }

  @Test
  void doSendNacksFallsBackToSubscriptionDefault() {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue, Duration.ofSeconds(5));

    AckInfo nack = new AckInfo(new AliSubscription.AliAckID("rh-1"), false);
    sub.doSendNacks(List.of(nack));

    verify(queue).changeMessageVisibility("rh-1", 5);
  }

  @Test
  void doSendNacksClampsToMaxVisibility() {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);

    AckInfo nack =
        new AckInfo(new AliSubscription.AliAckID("rh-1"), false, Duration.ofSeconds(100_000));
    sub.doSendNacks(List.of(nack));

    verify(queue).changeMessageVisibility("rh-1", 43200);
  }

  @Test
  void doSendNacksClampsDefaultZeroToMinVisibility() {
    // With no per-nack timeout and no subscription default, the base default is Duration.ZERO. SMQ
    // rejects a 0-second visibility timeout, so the lower-bound clamp must send 1, never 0.
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);

    AckInfo nack = new AckInfo(new AliSubscription.AliAckID("rh-1"), false);
    sub.doSendNacks(List.of(nack));

    verify(queue).changeMessageVisibility("rh-1", 1);
  }

  @Test
  void doSendAcksRejectsForeignAckIdType() throws Exception {
    // A non-AliAckID would stringify to a bogus handle that SMQ swallows as already-gone, so the
    // ack must fail fast rather than silently no-op and let the message loop forever.
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);
    AckID foreign = new AckID() {};

    assertThrows(InvalidArgumentException.class, () -> sub.doSendAcks(List.of(foreign)));
    verify(queue, never()).batchDeleteMessage(any());
  }

  @Test
  void doSendNacksRejectsForeignAckIdType() {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);
    AckInfo nack = new AckInfo(new AckID() {}, false);

    assertThrows(InvalidArgumentException.class, () -> sub.doSendNacks(List.of(nack)));
    verify(queue, never()).changeMessageVisibility(any(), anyInt());
  }

  @Test
  void foreignAckIdViaPublicSendAckSurfacesOnClose() throws Exception {
    // Public-API view of the wrong-type guard: a foreign AckID passes the base sendAck null-check
    // and is enqueued on the shared ack batcher, so the guard cannot reject it synchronously to the
    // caller. Draining the batcher on close() must surface that rejection, not swallow it. close()
    // re-throws the unreported ack error whose cause is the foreign-AckID InvalidArgumentException,
    // and the SMQ delete is never attempted for the bogus handle. Deterministic because
    // close() drains the ack batcher synchronously and re-throws the recorded permanent-error.
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscriptionWithClient(client, queue);

    sub.sendAck(new AckID() {});

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, sub::close);
    assertInstanceOf(InvalidArgumentException.class, thrown.getCause());
    verify(queue, never()).batchDeleteMessage(any());
  }

  @Test
  void doSendNacksIgnoresAlreadyGoneMessage() {
    CloudQueue queue = mock(CloudQueue.class);
    ServiceException gone = mock(ServiceException.class);
    when(queue.changeMessageVisibility(eq("rh-1"), anyInt())).thenThrow(gone);
    when(queue.isMessageNotExist(gone)).thenReturn(true);

    AliSubscription sub = subscription(queue);
    AckInfo nack = new AckInfo(new AliSubscription.AliAckID("rh-1"), false);
    assertDoesNotThrow(() -> sub.doSendNacks(List.of(nack)));
  }

  @Test
  void doSendNacksIgnoresExpiredReceiptHandle() {
    CloudQueue queue = mock(CloudQueue.class);
    ServiceException expired = mock(ServiceException.class);
    when(expired.getErrorCode()).thenReturn("ReceiptHandleError");
    when(queue.changeMessageVisibility(eq("rh-1"), anyInt())).thenThrow(expired);
    when(queue.isMessageNotExist(expired)).thenReturn(false);

    AliSubscription sub = subscription(queue);
    AckInfo nack = new AckInfo(new AliSubscription.AliAckID("rh-1"), false);
    assertDoesNotThrow(() -> sub.doSendNacks(List.of(nack)));
  }

  @Test
  void getAttributesReturnsQueueUrl() {
    CloudQueue queue = mock(CloudQueue.class);
    String url = "https://acct.mns.cn-x.aliyuncs.com/queues/test-queue";
    when(queue.getQueueURL()).thenReturn(url);

    AliSubscription sub = subscription(queue);
    GetAttributeResult attributes = sub.getAttributes();
    assertEquals("test-queue", attributes.getName());
    assertEquals(url, attributes.getTopic());
  }

  @Test
  void canNackIsTrueAndRetryabilityFollowsMappedException() {
    CloudQueue queue = mock(CloudQueue.class);
    AliSubscription sub = subscription(queue);
    assertTrue(sub.canNack());
    // A raw throwable carries no retryability signal and is treated as terminal.
    assertFalse(sub.isRetryable(new RuntimeException("x")));
    // A transient throttling error maps to a retryable ResourceExhaustedException.
    assertTrue(sub.isRetryable(new ResourceExhaustedException(new RuntimeException("throttled"))));
    // A non-retryable typed exception (e.g. authorization) stays terminal.
    assertFalse(sub.isRetryable(new UnAuthorizedException(new RuntimeException("denied"))));
  }

  @Test
  void aliAckIdRejectsBlankAndComparesByHandle() {
    assertThrows(IllegalArgumentException.class, () -> new AliSubscription.AliAckID(null));
    assertThrows(IllegalArgumentException.class, () -> new AliSubscription.AliAckID("  "));
    AckID a = new AliSubscription.AliAckID("rh");
    AckID b = new AliSubscription.AliAckID("rh");
    assertEquals("rh", a.toString());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void builderRequiresSubscriptionName() {
    MNSClient client = mock(MNSClient.class);
    AliSubscription.Builder builder = new AliSubscription.Builder();
    builder.withSmqClient(client);
    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void closeClosesSmqClientOnNormalPath() throws Exception {
    MNSClient client = mock(MNSClient.class);
    AliSubscription sub = subscriptionWithClient(client);

    sub.close();

    verify(client).close();
  }

  @Test
  void closeSurfacesSmqClientCloseFailureWhenShutdownSucceeds() {
    MNSClient client = mock(MNSClient.class);
    RuntimeException clientCloseError = new RuntimeException("client close failed");
    doThrow(clientCloseError).when(client).close();
    AliSubscription sub = subscriptionWithClient(client);

    // When shutdown succeeds, a client-close failure is not swallowed: it propagates directly.
    RuntimeException thrown = assertThrows(RuntimeException.class, sub::close);
    assertSame(clientCloseError, thrown);
  }

  @Test
  void closeClosesSmqClientAndPreservesPrimaryWhenShutdownFails() throws Exception {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    RuntimeException clientCloseError = new RuntimeException("client close failed");
    doThrow(clientCloseError).when(client).close();
    AliSubscription sub = subscriptionWithClient(client, queue);

    // Induce a super.close() failure organically through the public ack API: enqueue an ack, then
    // fail its delete with a non-benign error. Draining the pending ack during shutdown records an
    // unreported ack error that AbstractSubscription.close() re-throws as the primary exception.
    ServiceException denied = mock(ServiceException.class);
    when(denied.getErrorCode()).thenReturn("AccessDenied");
    doThrow(denied).when(queue).batchDeleteMessage(any());
    sub.sendAck(new AliSubscription.AliAckID("rh-1"));

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, sub::close);
    // The shutdown failure is surfaced as the primary exception; its cause is the mapped ack-drain
    // failure (AccessDenied -> UnAuthorizedException)...
    assertInstanceOf(UnAuthorizedException.class, thrown.getCause());
    // ...the SMQ client is still closed on the failure path (no leak)...
    verify(client).close();
    // ...and the client-close failure is attached as suppressed rather than masking the primary.
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(clientCloseError, thrown.getSuppressed()[0]);
  }

  @Test
  void withWaitTimeSecondsRejectsNegative() {
    AliSubscription.Builder builder = new AliSubscription.Builder();
    assertThrows(InvalidArgumentException.class, () -> builder.withWaitTimeSeconds(-1));
  }

  @Test
  void withWaitTimeSecondsAcceptsAndClampsAboveMax() throws Exception {
    CloudQueue queue = mock(CloudQueue.class);
    MNSClient client = mock(MNSClient.class);
    when(client.getQueueRef("test-queue")).thenReturn(queue);
    AliSubscription.Builder builder = new AliSubscription.Builder();
    builder.withSmqClient(client);
    builder.withSubscriptionName("test-queue");
    // A value above the SMQ maximum is accepted (no throw) and clamped to 30s.
    assertDoesNotThrow(() -> builder.withWaitTimeSeconds(60));
    AliSubscription sub = builder.build();
    closeables.add(sub);

    sub.doReceiveBatch(5);

    // The long-poll receive uses the clamped 30s wait, not the raw 60.
    verify(queue).batchPopMessage(5, 30);
  }

  private static AliSubscription subscriptionWithClient(MNSClient client) {
    return subscriptionWithClient(client, mock(CloudQueue.class));
  }

  private static AliSubscription subscriptionWithClient(MNSClient client, CloudQueue queue) {
    when(client.getQueueRef("test-queue")).thenReturn(queue);
    AliSubscription.Builder builder = new AliSubscription.Builder();
    builder.withSmqClient(client);
    builder.withSubscriptionName("test-queue");
    return builder.build();
  }

  /**
   * Round-trips an SMQ message through the SDK's real XML serialize then deserialize, exactly the
   * transform an SMQ receive applies over the wire, so a value serialized to XML text is exercised
   * through a genuine parse rather than an in-memory property object.
   */
  private static com.aliyun.mns.model.Message serializeThenDeserialize(
      com.aliyun.mns.model.Message message) throws Exception {
    try (InputStream serialized = new MessageSerializer().serialize(message, "UTF-8")) {
      return new MessageDeserializer().deserialize(serialized);
    }
  }

  @Test
  void xmlSafeMetadataValuesRoundTripAsStringThroughTheXmlWire() throws Exception {
    // XML-safe metadata values ride as native STRING user properties and survive a genuine
    // serialize -> deserialize round trip losslessly: plain ASCII, spaces, the XML special
    // characters, and multibyte UTF-8. The empty value is included as a regression guard that an
    // empty value still round-trips present-and-empty.
    assertMetadataValueRoundTrips("plain-ASCII-value");
    assertMetadataValueRoundTrips("value with spaces");
    assertMetadataValueRoundTrips("a & b < c > d");
    assertMetadataValueRoundTrips("世界");
    assertMetadataValueRoundTrips("");
  }

  @Test
  void xmlUnsafeMetadataValuesRoundTripAsBinaryThroughTheXmlWire() throws Exception {
    // Values raw XML text cannot carry -- an XML-illegal C0 control byte, the XML-1.0-illegal code
    // point U+FFFF -- ride as native BINARY user properties (base64 on the wire) and survive a
    // genuine serialize -> deserialize round trip losslessly. The SDK's BINARY path base64s the
    // value under the JVM default charset and reads it back as UTF-8, so the round trip is lossless
    // only on a UTF-8-default JVM (the supported, guarded configuration); assert it only there.
    assumeTrue(
        Charset.defaultCharset().equals(UTF_8),
        "BINARY metadata round trip is asserted only on a UTF-8-default JVM");
    assertMetadataValueRoundTrips("before\u0000after");
    assertMetadataValueRoundTrips("before\uFFFFafter");
  }

  /**
   * Publishes a one-entry-metadata message as a default-strategy publisher would, runs it through
   * the SDK's real XML serialize -> deserialize, receives it, and asserts the metadata value comes
   * back exactly. A value the SDK could not carry as raw XML text (a C0 control, U+FFFF) rides as
   * a native BINARY property, so a clean round trip proves the native STRING/BINARY encode/decode
   * pair.
   */
  private void assertMetadataValueRoundTrips(String value) throws Exception {
    Message original =
        Message.builder().withBody("body".getBytes(UTF_8)).withMetadata("meta", value).build();

    com.aliyun.mns.model.Message wire;
    MNSClient publisherClient = mock(MNSClient.class);
    when(publisherClient.getQueueRef("pub")).thenReturn(mock(CloudQueue.class));
    AliSmqQueue.Builder publisherBuilder = new AliSmqQueue.Builder();
    publisherBuilder.withSmqClient(publisherClient);
    publisherBuilder.withTopicName("pub");
    try (AliSmqQueue publisher = publisherBuilder.build()) {
      wire = serializeThenDeserialize(publisher.toSmqMessage(original));
    }
    wire.setReceiptHandle("rh");
    wire.setMessageId("mid");

    CloudQueue queue = mock(CloudQueue.class);
    when(queue.batchPopMessage(anyInt())).thenReturn(List.of(wire));
    AliSubscription sub = subscription(queue);
    Message received = sub.doReceiveBatch(10).get(0);

    assertEquals(
        value,
        received.getMetadata().get("meta"),
        "metadata value must round-trip losslessly through the XML wire: [" + value + "]");
  }
}
