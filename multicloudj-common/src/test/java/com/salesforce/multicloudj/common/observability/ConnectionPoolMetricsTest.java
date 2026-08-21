package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ConnectionPoolMetricsTest {

  @Test
  void fromProducesFourMetricsWithHttpClientCategory() {
    List<Metric> metrics = ConnectionPoolMetrics.from(100, 7, 20, 3);

    assertEquals(4, metrics.size());
    metrics.forEach(m -> assertEquals(ConnectionPoolMetrics.CATEGORY_HTTP_CLIENT, m.getCategory()));
  }

  @Test
  void fromMapsRawCountsToNamedMetrics() {
    List<Metric> metrics = ConnectionPoolMetrics.from(100, 7, 20, 3);
    Map<String, Object> byName =
        metrics.stream().collect(Collectors.toMap(Metric::getName, Metric::getValue));

    assertEquals(100, byName.get(ConnectionPoolMetrics.MAX_CONCURRENCY));
    assertEquals(7, byName.get(ConnectionPoolMetrics.LEASED_CONCURRENCY));
    assertEquals(20, byName.get(ConnectionPoolMetrics.AVAILABLE_CONCURRENCY));
    assertEquals(3, byName.get(ConnectionPoolMetrics.PENDING_CONCURRENCY_ACQUIRES));
  }

  @Test
  void fromHandlesZeroedPoolCounts() {
    List<Metric> metrics = ConnectionPoolMetrics.from(0, 0, 0, 0);
    Map<String, Object> byName =
        metrics.stream().collect(Collectors.toMap(Metric::getName, Metric::getValue));

    assertEquals(0, byName.get(ConnectionPoolMetrics.MAX_CONCURRENCY));
    assertEquals(0, byName.get(ConnectionPoolMetrics.LEASED_CONCURRENCY));
    assertEquals(0, byName.get(ConnectionPoolMetrics.AVAILABLE_CONCURRENCY));
    assertEquals(0, byName.get(ConnectionPoolMetrics.PENDING_CONCURRENCY_ACQUIRES));
  }
}
