package com.salesforce.multicloudj.blob.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricCollector;

class AwsMetricsPublisherAdapterTest {

  /** Simple capturing publisher used to assert what the adapter forwards. */
  private static final class CapturingPublisher implements MetricsPublisher {
    private final List<List<Metric>> batches = new ArrayList<>();
    private final AtomicInteger closeCount = new AtomicInteger();

    @Override
    public void publish(List<Metric> metrics) {
      batches.add(metrics);
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }

  /**
   * Builds a realistic AWS metric tree: an ApiCall root collection with a nested HttpClient child,
   * mirroring how the SDK reports connection-pool metrics under the HTTP layer.
   */
  private static MetricCollection buildApiCallTreeWithHttpChild() {
    MetricCollector root = MetricCollector.create("ApiCall");
    root.reportMetric(CoreMetric.API_CALL_DURATION, Duration.ofMillis(42));
    root.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");

    MetricCollector httpChild = root.createChild("HttpClient");
    httpChild.reportMetric(HttpMetric.MAX_CONCURRENCY, 100);
    httpChild.reportMetric(HttpMetric.LEASED_CONCURRENCY, 7);
    httpChild.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 3);

    return root.collect();
  }

  @Test
  void publishFlattensTreeAndTagsCategoryThenForwards() {
    CapturingPublisher publisher = new CapturingPublisher();
    AwsMetricsPublisherAdapter adapter = new AwsMetricsPublisherAdapter(publisher);

    adapter.publish(buildApiCallTreeWithHttpChild());

    assertEquals(1, publisher.batches.size());
    List<Metric> forwarded = publisher.batches.get(0);

    assertEquals(5, forwarded.size());

    Map<String, Metric> byName =
        forwarded.stream().collect(Collectors.toMap(Metric::getName, m -> m));

    assertEquals("ApiCall", byName.get(CoreMetric.API_CALL_DURATION.name()).getCategory());
    assertEquals(Duration.ofMillis(42), byName.get(CoreMetric.API_CALL_DURATION.name()).getValue());
    assertEquals("ApiCall", byName.get(CoreMetric.OPERATION_NAME.name()).getCategory());
    assertEquals("GetObject", byName.get(CoreMetric.OPERATION_NAME.name()).getValue());

    assertEquals("HttpClient", byName.get(HttpMetric.MAX_CONCURRENCY.name()).getCategory());
    assertEquals(100, byName.get(HttpMetric.MAX_CONCURRENCY.name()).getValue());
    assertEquals("HttpClient", byName.get(HttpMetric.LEASED_CONCURRENCY.name()).getCategory());
    assertEquals(7, byName.get(HttpMetric.LEASED_CONCURRENCY.name()).getValue());
    assertEquals(
        "HttpClient", byName.get(HttpMetric.PENDING_CONCURRENCY_ACQUIRES.name()).getCategory());
    assertEquals(3, byName.get(HttpMetric.PENDING_CONCURRENCY_ACQUIRES.name()).getValue());
  }

  @Test
  void publishWithNoChildrenForwardsOnlyRootMetrics() {
    CapturingPublisher publisher = new CapturingPublisher();
    AwsMetricsPublisherAdapter adapter = new AwsMetricsPublisherAdapter(publisher);

    MetricCollector root = MetricCollector.create("ApiCall");
    root.reportMetric(CoreMetric.RETRY_COUNT, 2);

    adapter.publish(root.collect());

    assertEquals(1, publisher.batches.size());
    List<Metric> forwarded = publisher.batches.get(0);
    assertEquals(1, forwarded.size());
    assertEquals(CoreMetric.RETRY_COUNT.name(), forwarded.get(0).getName());
    assertEquals(2, forwarded.get(0).getValue());
    assertEquals("ApiCall", forwarded.get(0).getCategory());
  }

  @Test
  void publishFlattensDeeplyNestedChildren() {
    CapturingPublisher publisher = new CapturingPublisher();
    AwsMetricsPublisherAdapter adapter = new AwsMetricsPublisherAdapter(publisher);

    MetricCollector root = MetricCollector.create("ApiCall");
    root.reportMetric(CoreMetric.OPERATION_NAME, "PutObject");
    MetricCollector attempt = root.createChild("ApiCallAttempt");
    attempt.reportMetric(CoreMetric.BACKOFF_DELAY_DURATION, Duration.ofMillis(10));
    MetricCollector http = attempt.createChild("HttpClient");
    http.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 5);

    adapter.publish(root.collect());

    List<Metric> forwarded = publisher.batches.get(0);
    assertEquals(3, forwarded.size());

    Map<String, String> categoryByName =
        forwarded.stream().collect(Collectors.toMap(Metric::getName, Metric::getCategory));
    assertEquals("ApiCall", categoryByName.get(CoreMetric.OPERATION_NAME.name()));
    assertEquals("ApiCallAttempt", categoryByName.get(CoreMetric.BACKOFF_DELAY_DURATION.name()));
    assertEquals("HttpClient", categoryByName.get(HttpMetric.AVAILABLE_CONCURRENCY.name()));
  }

  @Test
  void closeDelegatesToUnderlyingPublisher() {
    CapturingPublisher publisher = new CapturingPublisher();
    AwsMetricsPublisherAdapter adapter = new AwsMetricsPublisherAdapter(publisher);

    adapter.close();

    assertEquals(1, publisher.closeCount.get());
    assertTrue(publisher.batches.isEmpty());
  }
}
