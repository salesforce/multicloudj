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
 *   <li>{@code correlationId} — identifies a single logical request. If not supplied, the SDK
 *       generates a UUID and echoes it back on the response so the caller can correlate.
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
 * <p><b>Provider note:</b> the {@code ali} provider currently stamps only {@code correlationId}
 * onto stored objects; {@code serviceId} and {@code tenantId} are not yet persisted there. Callers
 * relying on object-metadata stamping for audit-log attribution should not assume service/tenant
 * ids are present on objects stored via {@code ali}.
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

  /**
   * Application-supplied service ID identifying the calling service. Optional; never
   * auto-generated. When provided, it is set as a {@code service_id} span attribute and MDC entry
   * for the duration of the operation, and is not echoed back in responses.
   */
  String serviceId;
}
