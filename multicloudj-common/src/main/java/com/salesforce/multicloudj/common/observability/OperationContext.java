package com.salesforce.multicloudj.common.observability;

import lombok.Builder;
import lombok.Value;

/**
 * Per-operation observability context attached to SDK requests.
 *
 * <p>Carries three optional identifiers used to correlate SDK activity with the caller's own
 * logs, traces, and audit records:
 *
 * <ul>
 *   <li>{@code correlationId} — identifies a single logical request. Optional and never
 *       auto-generated; when no context (or no correlation id) is supplied, it defaults to an
 *       empty string and tracing is treated as disabled for that operation. When provided, it is
 *       echoed back on the response so the caller can correlate.
 *   <li>{@code tenantId} — identifies the tenant on whose behalf the operation runs. Never
 *       auto-generated; never echoed back.
 *   <li>{@code serviceId} — identifies the calling service. Never auto-generated; never echoed
 *       back.
 * </ul>
 *
 * <p>When present, each identifier is set as a span attribute and MDC entry for the duration of
 * the operation. On upload, the {@code aws}, {@code gcp}, and {@code inmemory} providers
 * additionally stamp all three identifiers onto the stored object's metadata (under {@code
 * sdk-logging-*} keys) so cloud audit logs can be traced back to the originating request, tenant,
 * and service.
 *
 * <p>The metadata key under which the correlation id is stamped is customizable via {@code
 * correlationIdMetadataKey}; when it is not supplied the providers fall back to the default {@link
 * SdkLoggingMetadataKeys#CORRELATION_ID}. The stamped <em>value</em> is always {@code
 * correlationId}. This lets a caller align the stored key with an existing metadata convention
 * while leaving the service-id and tenant-id keys fixed.
 */
@Value
@Builder(toBuilder = true)
public class OperationContext {

  /**
   * Application-supplied correlation ID used to correlate this operation's logs and traces.
   * Optional; never auto-generated. If {@code null} or empty, it defaults to an empty string and
   * tracing is treated as disabled for that operation. When provided, it is echoed back via the
   * response object so the caller can correlate logs and traces.
   */
  String correlationId;

  /**
   * Application-supplied tenant ID. Optional; never auto-generated. When provided, it is set as
   * a {@code tenant_id} span attribute and MDC entry for the duration of the operation, and is
   * not echoed back in responses.
   */
  String tenantId;

  /**
   * Application-supplied service ID identifying the calling service. Optional; never
   * auto-generated. When provided, it is set as a {@code service_id} span attribute and MDC entry
   * for the duration of the operation, and is not echoed back in responses.
   */
  String serviceId;

  /**
   * Application-supplied metadata key under which providers stamp the {@link #correlationId} value
   * onto a stored object during upload. Optional; when {@code null} or blank, providers fall back
   * to the default {@link SdkLoggingMetadataKeys#CORRELATION_ID}. Only the correlation-id key is
   * customizable; the service-id and tenant-id keys remain fixed. This field affects only the
   * stored metadata <em>key</em>; it does not change the correlation id value, the span attribute,
   * the MDC entry, or the value echoed back on responses.
   */
  String correlationIdMetadataKey;

  /**
   * Resolves the metadata key under which the correlation id should be stamped on a stored object:
   * the application-supplied {@link #correlationIdMetadataKey} when present, otherwise the default
   * {@link SdkLoggingMetadataKeys#CORRELATION_ID}. Defined here so the fallback lives in one place
   * and cannot drift across provider implementations.
   *
   * @return the custom correlation-id metadata key when supplied and non-blank, else {@link
   *     SdkLoggingMetadataKeys#CORRELATION_ID}
   */
  public String resolveCorrelationIdMetadataKey() {
    return (correlationIdMetadataKey == null || correlationIdMetadataKey.trim().isEmpty())
        ? SdkLoggingMetadataKeys.CORRELATION_ID
        : correlationIdMetadataKey;
  }
}
