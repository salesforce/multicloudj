package com.salesforce.multicloudj.common.observability;

import lombok.Builder;
import lombok.Value;

/**
 * Per-operation observability context attached to SDK requests.
 *
 * <p>Carries the correlation ID and is the extension point for future per-call observability
 * metadata. When an {@code OperationContext} is attached to a request, the SDK uses its
 * correlation ID for the span attribute, the MDC, and echoes it back on the response (where
 * applicable). When no context (or no correlation ID) is supplied, the SDK generates a UUID.
 */
@Value
@Builder(toBuilder = true)
public class OperationContext {

  /**
   * Application-supplied correlation ID. If {@code null} or empty, the SDK generates a UUID and
   * returns it via the response object so the caller can correlate logs and traces.
   */
  String correlationId;

  /**
   * Application-supplied tenant ID. Optional; never auto-generated. When provided, it is set as
   * a {@code tenant_id} span attribute and MDC entry for the duration of the operation, and is
   * not echoed back in responses.
   */
  String tenantId;
}
