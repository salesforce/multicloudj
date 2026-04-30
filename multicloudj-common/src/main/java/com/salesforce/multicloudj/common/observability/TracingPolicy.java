package com.salesforce.multicloudj.common.observability;

/**
 * Policy controlling whether SDK operations create OpenTelemetry spans.
 *
 * <p>The default policy for a client is {@link #DISABLED} (zero behavior change for existing
 * users). The default can be overridden globally via {@link
 * MultiCloudJObservability#setDefaultTracingPolicy(TracingPolicy)}, and per-client via the
 * client's builder.
 */
public enum TracingPolicy {
  /**
   * No spans are created. Correlation ID, sdk_service, and sdk_provider are still set in MDC so
   * application logs always carry them regardless of the tracing policy.
   */
  DISABLED,

  /** A child span is created only when an application trace is already active. */
  JOIN_ONLY,

  /** A span is always created; a root span is started if no parent trace exists. */
  CHILD_AND_ROOT
}
