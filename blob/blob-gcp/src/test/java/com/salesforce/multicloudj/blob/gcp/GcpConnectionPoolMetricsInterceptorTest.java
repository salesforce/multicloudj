package com.salesforce.multicloudj.blob.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.observability.ConnectionPoolMetrics;
import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.pool.PoolStats;
import org.junit.jupiter.api.Test;

class GcpConnectionPoolMetricsInterceptorTest {

  /** Capturing publisher used to assert what the interceptor forwards. */
  private static final class CapturingPublisher implements MetricsPublisher {
    private final List<List<Metric>> batches = new ArrayList<>();

    @Override
    public void publish(List<Metric> metrics) {
      batches.add(metrics);
    }
  }

  @Test
  void processPublishesCurrentPoolStats() {
    CapturingPublisher publisher = new CapturingPublisher();
    PoolStats stats = new PoolStats(7, 3, 20, 100);
    GcpConnectionPoolMetricsInterceptor interceptor =
        new GcpConnectionPoolMetricsInterceptor(() -> stats, publisher);

    interceptor.process(null, null);

    assertEquals(1, publisher.batches.size());
    Map<String, Object> byName =
        publisher.batches.get(0).stream()
            .collect(Collectors.toMap(Metric::getName, Metric::getValue));
    assertEquals(100, byName.get(ConnectionPoolMetrics.MAX_CONCURRENCY));
    assertEquals(7, byName.get(ConnectionPoolMetrics.LEASED_CONCURRENCY));
    assertEquals(20, byName.get(ConnectionPoolMetrics.AVAILABLE_CONCURRENCY));
    assertEquals(3, byName.get(ConnectionPoolMetrics.PENDING_CONCURRENCY_ACQUIRES));
  }

  @Test
  void processIsNoOpWhenPublisherIsNull() {
    GcpConnectionPoolMetricsInterceptor interceptor =
        new GcpConnectionPoolMetricsInterceptor(() -> new PoolStats(1, 1, 1, 1), null);
    interceptor.process(null, null);
  }

  @Test
  void processIsNoOpWhenPoolStatsSupplierIsNull() {
    CapturingPublisher publisher = new CapturingPublisher();
    GcpConnectionPoolMetricsInterceptor interceptor =
        new GcpConnectionPoolMetricsInterceptor(null, publisher);

    interceptor.process(null, null);

    assertTrue(publisher.batches.isEmpty());
  }

  @Test
  void processIsNoOpWhenPoolStatsAreNull() {
    CapturingPublisher publisher = new CapturingPublisher();
    GcpConnectionPoolMetricsInterceptor interceptor =
        new GcpConnectionPoolMetricsInterceptor(() -> null, publisher);

    interceptor.process(null, null);

    assertTrue(publisher.batches.isEmpty());
  }

  @Test
  void processSwallowsSupplierFailure() {
    CapturingPublisher publisher = new CapturingPublisher();
    GcpConnectionPoolMetricsInterceptor interceptor =
        new GcpConnectionPoolMetricsInterceptor(
            () -> {
              throw new IllegalStateException("pool unavailable");
            },
            publisher);

    interceptor.process(null, null);
    assertTrue(publisher.batches.isEmpty());
  }
}
