package com.salesforce.multicloudj.common.observability;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.regex.Pattern;
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
   *
   * <p>When supplied and non-blank, the key must be a valid object-metadata key: it must contain
   * only ASCII letters, digits, hyphens and underscores, and must not equal one of the SDK's
   * reserved keys ({@link SdkLoggingMetadataKeys#SERVICE_ID} or {@link
   * SdkLoggingMetadataKeys#TENANT_ID}) — reusing a reserved key would collide with the fixed
   * service-id/tenant-id stamps and drop them. A key that violates either rule is rejected with an
   * {@link InvalidArgumentException} when it is resolved during upload.
   */
  String correlationIdMetadataKey;

  /**
   * Allowed shape for a custom correlation-id metadata key: ASCII letters, digits, hyphens and
   * underscores only. This is the intersection that AWS S3, GCS and OSS all accept as a
   * user-metadata key name, so a key that passes here is portable across every provider.
   */
  private static final Pattern VALID_METADATA_KEY = Pattern.compile("[A-Za-z0-9_-]+");

  /**
   * Resolves the metadata key under which the correlation id should be stamped on a stored object:
   * the application-supplied {@link #correlationIdMetadataKey} when present, otherwise the default
   * {@link SdkLoggingMetadataKeys#CORRELATION_ID}. Defined here so the fallback and validation live
   * in one place and cannot drift across provider implementations.
   *
   * <p>When a custom key is supplied, it is validated before being returned: it must match {@link
   * #VALID_METADATA_KEY} and must not equal a reserved key ({@link
   * SdkLoggingMetadataKeys#SERVICE_ID} or {@link SdkLoggingMetadataKeys#TENANT_ID}). This fails
   * fast with a clear SDK exception rather than surfacing a substrate error later or silently
   * dropping the service-id/tenant-id stamp on a collision.
   *
   * @return the custom correlation-id metadata key when supplied and non-blank, else {@link
   *     SdkLoggingMetadataKeys#CORRELATION_ID}
   * @throws InvalidArgumentException if a supplied custom key has an invalid shape or collides with
   *     a reserved key
   */
  public String resolveCorrelationIdMetadataKey() {
    if (correlationIdMetadataKey == null || correlationIdMetadataKey.trim().isEmpty()) {
      return SdkLoggingMetadataKeys.CORRELATION_ID;
    }
    if (SdkLoggingMetadataKeys.SERVICE_ID.equals(correlationIdMetadataKey)
        || SdkLoggingMetadataKeys.TENANT_ID.equals(correlationIdMetadataKey)) {
      throw new InvalidArgumentException(
          "correlationIdMetadataKey must not equal a reserved SDK metadata key ('"
              + SdkLoggingMetadataKeys.SERVICE_ID
              + "' or '"
              + SdkLoggingMetadataKeys.TENANT_ID
              + "'): "
              + correlationIdMetadataKey);
    }
    if (!VALID_METADATA_KEY.matcher(correlationIdMetadataKey).matches()) {
      throw new InvalidArgumentException(
          "correlationIdMetadataKey may contain only ASCII letters, digits, hyphens and"
              + " underscores: "
              + correlationIdMetadataKey);
    }
    return correlationIdMetadataKey;
  }
}
