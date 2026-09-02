package com.salesforce.multicloudj.pubsub.ali;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.BatchSendException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.ErrorMessageResult;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AliQueueTopicTest {

  private final List<AutoCloseable> closeables = new ArrayList<>();

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable c : closeables) {
      c.close();
    }
    closeables.clear();
  }

  private AliQueueTopic topic(MNSClient client, CloudQueue queue) {
    when(client.getQueueRef("test-queue")).thenReturn(queue);
    AliQueueTopic.Builder builder = new AliQueueTopic.Builder();
    builder.withMnsClient(client);
    builder.withTopicName("test-queue");
    AliQueueTopic topic = builder.build();
    closeables.add(topic);
    return topic;
  }

  @Test
  void getProviderIdIsAliSmqQueue() throws Exception {
    try (AliQueueTopic t = new AliQueueTopic()) {
      assertEquals("alismqqueue", t.getProviderId());
    }
  }

  @Test
  void sendBodyOnlyMessagePutsBase64BodyToQueue() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    topic.send(Message.builder().withBody("hello".getBytes(UTF_8)).build());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<com.aliyun.mns.model.Message>> captor = ArgumentCaptor.forClass(List.class);
    verify(queue).batchPutMessage(captor.capture());
    List<com.aliyun.mns.model.Message> sent = captor.getValue();
    assertEquals(1, sent.size());
    assertArrayEquals("hello".getBytes(UTF_8), sent.get(0).getMessageBodyAsBytes());
  }

  @Test
  void sendMessageWithMetadataThrowsUnsupportedAndDoesNotPublish() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    Message message =
        Message.builder().withBody("x".getBytes(UTF_8)).withMetadata("k", "v").build();
    assertThrows(UnSupportedOperationException.class, () -> topic.send(message));
    verify(queue, never()).batchPutMessage(any());
  }

  @Test
  void serviceExceptionIsMappedToTypedException() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    ServiceException denied = mock(ServiceException.class);
    when(denied.getErrorCode()).thenReturn("AccessDenied");
    when(queue.batchPutMessage(any())).thenThrow(denied);

    assertThrows(
        UnAuthorizedException.class,
        () -> topic.send(Message.builder().withBody("x".getBytes(UTF_8)).build()));
  }

  @Test
  void perMessageFailedEntryIsSurfaced() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    // SMQ throws BatchSendException (never returns a flagged list) when a message in the batch
    // fails; the exception carries the per-message error detail.
    ErrorMessageResult error = new ErrorMessageResult();
    error.setErrorCode("QueueNotExist");
    error.setErrorMessage("no such queue");
    com.aliyun.mns.model.Message failed = mock(com.aliyun.mns.model.Message.class);
    when(failed.isErrorMessage()).thenReturn(true);
    when(failed.getErrorMessageDetail()).thenReturn(error);
    when(queue.batchPutMessage(any())).thenThrow(new BatchSendException(List.of(failed)));

    assertThrows(
        ResourceNotFoundException.class,
        () -> topic.send(Message.builder().withBody("x".getBytes(UTF_8)).build()));
  }

  @Test
  void doSendBatchFailsWholeBatchOnMixedBatchSendResult() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    // SMQ's batchPutMessage may accept some messages while rejecting others; on any failure it
    // throws BatchSendException whose result list mixes accepted entries (isErrorMessage()==false)
    // with rejected ones. Build such a mixed list with the real SDK API: a plain Message is
    // accepted (no ErrorMessageResult set), while setErrorMessage(...) flags an entry as rejected.
    com.aliyun.mns.model.Message accepted = new com.aliyun.mns.model.Message();

    ErrorMessageResult firstError = new ErrorMessageResult();
    firstError.setErrorCode("QueueNotExist");
    firstError.setErrorMessage("no such queue");
    com.aliyun.mns.model.Message firstFailed = new com.aliyun.mns.model.Message();
    firstFailed.setErrorMessage(firstError);

    ErrorMessageResult secondError = new ErrorMessageResult();
    secondError.setErrorCode("AccessDenied");
    secondError.setErrorMessage("denied");
    com.aliyun.mns.model.Message secondFailed = new com.aliyun.mns.model.Message();
    secondFailed.setErrorMessage(secondError);

    when(queue.batchPutMessage(any()))
        .thenThrow(new BatchSendException(List.of(accepted, firstFailed, secondFailed)));

    List<Message> batch = List.of(Message.builder().withBody("x".getBytes(UTF_8)).build());
    // The accepted entry does not suppress the failure: the whole batch fails, mapped from the
    // FIRST rejected entry (QueueNotExist -> ResourceNotFoundException), not the second
    // (AccessDenied -> UnAuthorizedException).
    assertThrows(ResourceNotFoundException.class, () -> topic.doSendBatch(batch));
  }

  @Test
  void mappedPartialBatchFailureRetainsOriginalBatchSendExceptionInCausalChain() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topic(client, queue);

    ErrorMessageResult error = new ErrorMessageResult();
    error.setErrorCode("QueueNotExist");
    error.setErrorMessage("no such queue");
    com.aliyun.mns.model.Message accepted = new com.aliyun.mns.model.Message();
    com.aliyun.mns.model.Message failed = new com.aliyun.mns.model.Message();
    failed.setErrorMessage(error);
    BatchSendException batchException = new BatchSendException(List.of(accepted, failed));
    when(queue.batchPutMessage(any())).thenThrow(batchException);

    List<Message> batch = List.of(Message.builder().withBody("x".getBytes(UTF_8)).build());
    ResourceNotFoundException thrown =
        assertThrows(ResourceNotFoundException.class, () -> topic.doSendBatch(batch));

    // The mapped exception must not discard the original BatchSendException: it carries the full
    // per-entry result list, SDK request context, and original stack, so it stays reachable in the
    // causal chain (mapped exception -> diagnostic RuntimeException -> BatchSendException).
    assertSame(batchException, findCause(thrown, BatchSendException.class));
  }

  @Test
  void builderRequiresTopicName() {
    MNSClient client = mock(MNSClient.class);
    AliQueueTopic.Builder builder = new AliQueueTopic.Builder();
    builder.withMnsClient(client);
    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void builderRequiresEndpointWhenNoClientInjected() {
    AliQueueTopic.Builder builder = new AliQueueTopic.Builder();
    builder.withTopicName("test-queue");
    // No MNSClient and no endpoint -> client construction rejects the missing endpoint.
    assertThrows(InvalidArgumentException.class, builder::build);
  }

  @Test
  void closeClosesMnsClientOnNormalPath() throws Exception {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    AliQueueTopic topic = topicWithClient(client, queue);

    topic.close();

    verify(client).close();
  }

  @Test
  void closeSurfacesMnsClientCloseFailureWhenShutdownSucceeds() {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    RuntimeException clientCloseError = new RuntimeException("client close failed");
    doThrow(clientCloseError).when(client).close();
    AliQueueTopic topic = topicWithClient(client, queue);

    // When shutdown succeeds, a client-close failure is not swallowed: it propagates directly.
    RuntimeException thrown = assertThrows(RuntimeException.class, topic::close);
    assertSame(clientCloseError, thrown);
  }

  @Test
  void closeClosesMnsClientAndPreservesPrimaryWhenShutdownFails() throws Exception {
    MNSClient client = mock(MNSClient.class);
    CloudQueue queue = mock(CloudQueue.class);
    RuntimeException clientCloseError = new RuntimeException("client close failed");
    doThrow(clientCloseError).when(client).close();
    AliQueueTopic topic = topicWithClient(client, queue);

    // Induce a super.close() failure. This drain failure cannot be provoked through the public
    // send API: send() is synchronous and fully drains before returning, AbstractTopic.close()
    // marks the topic shut down before draining so any pending batch short-circuits, and the
    // Batcher captures every handler failure into its item futures rather than propagating it out
    // of shutdownAndDrain(). The failure is therefore injected by mocking shutdownAndDrain().
    RuntimeException flushError = new RuntimeException("flush failed");
    @SuppressWarnings("unchecked")
    Batcher<Message> batcher = mock(Batcher.class);
    doThrow(flushError).when(batcher).shutdownAndDrain();
    setBatcher(topic, batcher);

    RuntimeException thrown = assertThrows(RuntimeException.class, topic::close);
    // The shutdown failure is surfaced as the primary exception...
    assertSame(flushError, thrown);
    // ...the MNS client is still closed on the failure path (no leak)...
    verify(client).close();
    // ...and the client-close failure is attached as suppressed rather than masking the primary.
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(clientCloseError, thrown.getSuppressed()[0]);
  }

  private static AliQueueTopic topicWithClient(MNSClient client, CloudQueue queue) {
    when(client.getQueueRef("test-queue")).thenReturn(queue);
    AliQueueTopic.Builder builder = new AliQueueTopic.Builder();
    builder.withMnsClient(client);
    builder.withTopicName("test-queue");
    return builder.build();
  }

  private static void setBatcher(AliQueueTopic topic, Batcher<Message> batcher) throws Exception {
    Field field = AbstractTopic.class.getDeclaredField("batcher");
    field.setAccessible(true);
    field.set(topic, batcher);
  }

  private static <T extends Throwable> T findCause(Throwable throwable, Class<T> type) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
    }
    return null;
  }
}
