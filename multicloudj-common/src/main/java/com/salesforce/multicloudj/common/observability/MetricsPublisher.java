package com.salesforce.multicloudj.common.observability;

import java.util.List;

/**
 * Cloud-agnostic sink for client-level metrics.
 *
 * <p>Substrate clients (blob, docstore, etc.) collect metrics from the underlying cloud SDK and,
 * when a publisher is configured, forward them here as neutral {@link Metric} instances. This
 * surface intentionally has no dependency on any provider's telemetry API so a single operator
 * implementation can receive metrics regardless of which cloud is backing the client.
 *
 * <p>Implementations may be invoked concurrently from multiple threads and must be thread-safe.
 * The {@link #publish(List)} method should return quickly, deferring any expensive aggregation or
 * transmission to a background thread, and must never propagate an exception to the caller.
 */
public interface MetricsPublisher {

  /**
   * Notifies the publisher of a batch of newly collected metrics.
   *
   * @param metrics the metrics collected for a single operation; never {@code null}
   */
  void publish(List<Metric> metrics);

  /**
   * Releases any resources held by the publisher. The default implementation does nothing.
   *
   * <p>Called when the owning client is closed.
   */
  default void close() {}
}
