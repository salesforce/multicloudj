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
   * No span is created.
   *
   * <p>MDC is populated with {@code correlation_id}, {@code sdk_service}, {@code sdk_provider},
   * and (when supplied) {@code tenant_id} so application logs always carry them regardless of
   * the tracing policy. {@code trace_id}/{@code span_id} are NOT set in MDC.
   */
  DISABLED,

  /**
   * A child span is created only when an application trace is already active (i.e. a valid span
   * exists on the current OpenTelemetry context).
   *
   * <p>If a parent is present: a child span is created and full MDC ({@code trace_id}, {@code
   * span_id}, plus the {@link #DISABLED} fields) is populated.
   *
   * <p>If no parent is present: behaves identically to {@link #DISABLED} -- no span, MDC carries
   * only the correlation/service/provider/tenant fields.
   */
  JOIN_ONLY,

  /**
   * A span is always created.
   *
   * <p>If a parent is present: a child span is created joining the application's trace.
   *
   * <p>If no parent is present: a new root span is started (fresh trace ID).
   *
   * <p>In both cases full MDC ({@code trace_id}, {@code span_id}, plus the {@link #DISABLED}
   * fields) is populated for the duration of the operation.
   */
  CHILD_AND_ROOT
}
