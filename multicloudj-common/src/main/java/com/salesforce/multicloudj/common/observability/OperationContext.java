package com.salesforce.multicloudj.common.observability;

import lombok.Builder;
import lombok.Value;

/**
 * Per-operation observability context attached to SDK requests.
 *
 * <p>The SDK does not hard-code an external name for the correlation id. The caller decides the
 * name via {@link #correlationIdKey} (e.g. {@code "X-Request-Id"}, {@code "logging-id"},
 * {@code "sdk-correlation-id"}); the SDK uses that same name as the MDC key, the OpenTelemetry
 * span attribute name, and the stored blob metadata key. The Java field {@link #correlationId}
 * holds the value.
 *
 * <p>Resolution flow when {@link #correlationIdKey} is set:
 *
 * <ol>
 *   <li>If {@link #correlationId} is set on the context, use it.
 *   <li>Else if SLF4J {@code MDC} already has an entry under the key (typical when the caller's
 *       request filter populated MDC), the SDK reads it from there.
 *   <li>Else the SDK generates a UUID and stamps it under the key in MDC, span attributes, and
 *       stored blob metadata, and echoes it back on the response.
 * </ol>
 *
 * <p>When {@link #correlationIdKey} is {@code null} or empty, the SDK does not surface any
 * correlation id in MDC, span attributes, or stored blob metadata (the caller has opted out of
 * external surfacing); {@code response.getCorrelationId()} will be {@code null} unless the
 * caller supplied {@link #correlationId} explicitly.
 */
@Value
@Builder(toBuilder = true)
public class OperationContext {

  /**
   * Application-supplied correlation ID value. May be {@code null}; see resolution flow on the
   * class Javadoc.
   */
  String correlationId;

  /**
   * Application-supplied name under which the correlation id is identified in the caller's
   * environment. The SDK uses this exact name as the MDC key, the OpenTelemetry span attribute
   * name, and the stored blob metadata key. When {@code null} or empty, the SDK does not
   * surface a correlation id on any of those external channels.
   */
  String correlationIdKey;

  /**
   * Application-supplied tenant ID. Optional; never auto-generated. When provided, it is set as
   * a {@code tenant_id} span attribute and MDC entry for the duration of the operation, and is
   * not echoed back in responses.
   */
  String tenantId;
}
