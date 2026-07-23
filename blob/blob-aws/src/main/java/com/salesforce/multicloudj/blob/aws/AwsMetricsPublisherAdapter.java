package com.salesforce.multicloudj.blob.aws;

import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricRecord;

/**
 * Bridges the AWS SDK's {@link software.amazon.awssdk.metrics.MetricPublisher} SPI to the
 * cloud-agnostic {@link MetricsPublisher}.
 *
 * <p>The AWS SDK reports metrics as a tree of {@link MetricCollection}s: a root collection for the
 * API call with nested child collections for lower layers such as the HTTP client (where
 * connection-pool metrics like {@code MaxConcurrency}, {@code LeasedConcurrency}, {@code
 * PendingConcurrencyAcquires}, and {@code ConcurrencyAcquireDuration} live). This adapter flattens
 * that tree into neutral {@link Metric} instances, tagging each with the name of the collection
 * that produced it, and forwards them to the configured {@link MetricsPublisher}.
 */
public class AwsMetricsPublisherAdapter implements software.amazon.awssdk.metrics.MetricPublisher {

  private final MetricsPublisher delegate;

  public AwsMetricsPublisherAdapter(MetricsPublisher delegate) {
    this.delegate = delegate;
  }

  @Override
  public void publish(MetricCollection metricCollection) {
    List<Metric> metrics = new ArrayList<>();
    flatten(metricCollection, metrics);
    delegate.publish(metrics);
  }

  private void flatten(MetricCollection collection, List<Metric> out) {
    String category = collection.name();
    for (MetricRecord<?> record : collection) {
      out.add(
          Metric.builder()
              .name(record.metric().name())
              .value(record.value())
              .category(category)
              .build());
    }
    for (MetricCollection child : collection.children()) {
      flatten(child, out);
    }
  }

  @Override
  public void close() {
    delegate.close();
  }
}
