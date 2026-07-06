package com.salesforce.multicloudj.blob.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.aliyun.sdk.service.oss2.transport.RequestContext;
import com.aliyun.sdk.service.oss2.transport.RequestMessage;
import com.aliyun.sdk.service.oss2.transport.ResponseMessage;
import com.salesforce.multicloudj.common.observability.ConnectionPoolMetrics;
import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.hc.core5.pool.PoolStats;
import org.junit.jupiter.api.Test;

class AliConnectionPoolMetricsHttpClientTest {

  private static final class CapturingPublisher implements MetricsPublisher {
    private final List<List<Metric>> batches = new ArrayList<>();

    @Override
    public void publish(List<Metric> metrics) {
      batches.add(metrics);
    }
  }

  @Test
  void sendSamplesSyncPoolAndReturnsDelegateResponse() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    ResponseMessage response = mock(ResponseMessage.class);
    when(delegate.send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(response);

    PoolStats syncStats = new PoolStats(5, 2, 10, 50);
    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(
            delegate, publisher, () -> syncStats, () -> new PoolStats(0, 0, 0, 0));

    ResponseMessage result = client.send(mock(RequestMessage.class), RequestContext.empty());

    assertSame(response, result);
    assertEquals(1, publisher.batches.size());
    Map<String, Object> byName =
        publisher.batches.get(0).stream()
            .collect(Collectors.toMap(Metric::getName, Metric::getValue));
    assertEquals(50, byName.get(ConnectionPoolMetrics.MAX_CONCURRENCY));
    assertEquals(5, byName.get(ConnectionPoolMetrics.LEASED_CONCURRENCY));
    assertEquals(10, byName.get(ConnectionPoolMetrics.AVAILABLE_CONCURRENCY));
    assertEquals(2, byName.get(ConnectionPoolMetrics.PENDING_CONCURRENCY_ACQUIRES));
  }

  @Test
  void sendSamplesSyncPoolEvenWhenDelegateThrows() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    when(delegate.send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("boom"));

    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(
            delegate, publisher, () -> new PoolStats(1, 0, 4, 10), null);

    try {
      client.send(mock(RequestMessage.class), RequestContext.empty());
    } catch (RuntimeException expected) {
      // delegate failure propagates; metrics must still have been sampled in finally
    }
    assertEquals(1, publisher.batches.size());
  }

  @Test
  void sendAsyncSamplesAsyncPoolOnCompletion() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    ResponseMessage response = mock(ResponseMessage.class);
    when(delegate.sendAsync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    PoolStats asyncStats = new PoolStats(9, 1, 6, 40);
    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(
            delegate, publisher, () -> new PoolStats(0, 0, 0, 0), () -> asyncStats);

    ResponseMessage result =
        client.sendAsync(mock(RequestMessage.class), RequestContext.empty()).join();

    assertSame(response, result);
    assertEquals(1, publisher.batches.size());
    Map<String, Object> byName =
        publisher.batches.get(0).stream()
            .collect(Collectors.toMap(Metric::getName, Metric::getValue));
    assertEquals(40, byName.get(ConnectionPoolMetrics.MAX_CONCURRENCY));
    assertEquals(9, byName.get(ConnectionPoolMetrics.LEASED_CONCURRENCY));
  }

  @Test
  void sendAsyncSamplesAsyncPoolWhenFutureFails() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    CompletableFuture<ResponseMessage> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("network"));
    when(delegate.sendAsync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(failed);

    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(
            delegate, publisher, null, () -> new PoolStats(2, 0, 3, 20));

    try {
      client.sendAsync(mock(RequestMessage.class), RequestContext.empty()).join();
    } catch (CompletionException expected) {
      // failure propagates to caller; metrics still sampled on completion
    }
    assertEquals(1, publisher.batches.size());
  }

  @Test
  void sendIsNoOpWhenSyncSupplierIsNull() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    when(delegate.send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(ResponseMessage.class));

    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(delegate, publisher, null, null);

    client.send(mock(RequestMessage.class), RequestContext.empty());
    assertTrue(publisher.batches.isEmpty());
  }

  @Test
  void sendSwallowsPoolSamplingFailure() {
    CapturingPublisher publisher = new CapturingPublisher();
    HttpClient delegate = mock(HttpClient.class);
    when(delegate.send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(ResponseMessage.class));

    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(
            delegate,
            publisher,
            () -> {
              throw new IllegalStateException("pool closed");
            },
            null);

    client.send(mock(RequestMessage.class), RequestContext.empty());
    assertTrue(publisher.batches.isEmpty());
  }

  @Test
  void nameDelegates() {
    HttpClient delegate = mock(HttpClient.class);
    when(delegate.name()).thenReturn("apache5-mixed");
    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(delegate, new CapturingPublisher(), null, null);
    assertEquals("apache5-mixed", client.name());
  }

  /** A closeable transport used to assert that close() propagates to the delegate. */
  private interface CloseableHttpClient extends HttpClient, AutoCloseable {}

  @Test
  void closePropagatesToCloseableDelegate() throws Exception {
    CloseableHttpClient delegate = mock(CloseableHttpClient.class);
    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(delegate, new CapturingPublisher(), null, null);

    client.close();

    verify(delegate).close();
  }

  @Test
  void closeIsNoOpWhenDelegateNotCloseable() throws Exception {
    HttpClient delegate = mock(HttpClient.class);
    AliConnectionPoolMetricsHttpClient client =
        new AliConnectionPoolMetricsHttpClient(delegate, new CapturingPublisher(), null, null);

    client.close();
  }
}
