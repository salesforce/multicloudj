package com.salesforce.multicloudj.common.observability;

import lombok.Builder;
import lombok.Getter;

/**
 * A single cloud-agnostic metric data point emitted by a substrate client.
 *
 * <p>This is a neutral value object with no dependency on any cloud provider's telemetry API. A
 * provider implementation is responsible for translating its native metric events into instances
 * of this type before handing them to a {@link MetricsPublisher}.
 *
 * <p>The {@link #category} preserves the logical layer that produced the metric (for example {@code
 * "ApiCall"} or {@code "HttpClient"}) so operators can distinguish, e.g., connection-pool
 * saturation metrics from request-level metrics without needing provider-specific types.
 *
 * <p><b>Provider-neutral contract.</b> The only metrics guaranteed on every supported provider are
 * the four connection-pool counters defined in {@link ConnectionPoolMetrics} ({@code
 * MaxConcurrency}, {@code LeasedConcurrency}, {@code AvailableConcurrency}, {@code
 * PendingConcurrencyAcquires}), all emitted under {@link ConnectionPoolMetrics#CATEGORY_HTTP_CLIENT
 * "HttpClient"} with an {@link Integer} {@link #value}. Any other name/category (for example an
 * AWS-shaped {@code ApiCall}/{@code ApiCallAttempt} metric, whose value may be a {@code Duration},
 * {@code String}, or numeric type) is a provider-specific superset that is not portable across
 * clouds. A consumer written to the neutral contract should filter by category {@code "HttpClient"}
 * and the four names above, and must not assume the {@link #value} type of any other metric.
 */
@Builder
@Getter
public class Metric {

  /** The name of the metric, for example {@code "MaxConcurrency"} or {@code "ApiCallDuration"}. */
  private final String name;

  /** The recorded value of the metric. */
  private final Object value;

  /**
   * The logical layer that produced this metric (for example {@code "ApiCall"} or {@code
   * "HttpClient"}). May be {@code null} when the producing layer is unknown.
   */
  private final String category;
}
