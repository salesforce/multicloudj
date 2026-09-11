package com.salesforce.multicloudj.common.observability;

import java.util.Arrays;
import java.util.List;

/**
 * Helper for producing cloud-agnostic connection-pool utilization {@link Metric}s.
 *
 * <p>HTTP connection pools expose a small, universal set of counters — the maximum number of
 * connections the pool may hold, how many are currently leased (in use), how many are idle and
 * available for reuse, and how many callers are blocked waiting to acquire one. This helper
 * translates those raw counts into neutral {@link Metric} instances tagged with the {@link
 * #CATEGORY_HTTP_CLIENT} category so that connection-pool saturation can be observed uniformly
 * regardless of which cloud SDK produced the numbers.
 *
 * <p>The metric names mirror the vocabulary a substrate client already emits for the HTTP layer,
 * so operators see a single, consistent set of names across providers.
 */
public final class ConnectionPoolMetrics {

  /** Category tagged on every connection-pool metric, identifying the HTTP client layer. */
  public static final String CATEGORY_HTTP_CLIENT = "HttpClient";

  /** Maximum number of connections the pool is configured to hold. */
  public static final String MAX_CONCURRENCY = "MaxConcurrency";

  /** Number of connections currently leased (checked out and in use). */
  public static final String LEASED_CONCURRENCY = "LeasedConcurrency";

  /** Number of idle connections currently available for reuse. */
  public static final String AVAILABLE_CONCURRENCY = "AvailableConcurrency";

  /** Number of callers currently blocked waiting to acquire a connection. */
  public static final String PENDING_CONCURRENCY_ACQUIRES = "PendingConcurrencyAcquires";

  private ConnectionPoolMetrics() {}

  public static List<Metric> from(
      int maxConcurrency,
      int leasedConcurrency,
      int availableConcurrency,
      int pendingConcurrencyAcquires) {
    return Arrays.asList(
        metric(MAX_CONCURRENCY, maxConcurrency),
        metric(LEASED_CONCURRENCY, leasedConcurrency),
        metric(AVAILABLE_CONCURRENCY, availableConcurrency),
        metric(PENDING_CONCURRENCY_ACQUIRES, pendingConcurrencyAcquires));
  }

  private static Metric metric(String name, int value) {
    return Metric.builder().name(name).value(value).category(CATEGORY_HTTP_CLIENT).build();
  }
}
