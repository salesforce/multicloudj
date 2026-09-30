package com.salesforce.multicloudj.common.observability;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

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
 * additionally stamp all three identifiers onto the stored object's metadata. By default, the
 * correlation id is stored under {@code sdk-logging-correlation-id} (metadata key) and {@code
 * correlation_id} (MDC + span attribute); tenant and service ids are stored under {@code
 * sdk-logging-tenant-id} and {@code sdk-logging-service-id}. A custom {@code correlationIdKey}
 * replaces both defaults on all three surfaces (metadata, MDC, span), allowing callers to
 * align the SDK's tracing with their own naming conventions.
 */
@Value
@Builder(toBuilder = true)
public class OperationContext {

  /**
   * Regex pattern for validating custom correlation ID key names. Lowercase alphanumeric only to
   * prevent casing drift (S3 and GCS lowercase user-metadata keys on read). Safe subset of RFC
   * 7230 header tokens that also works as MDC key and OTel attribute name.
   */
  private static final Pattern VALID_KEY_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,127}$");

  /**
   * Reserved key names that cannot be used as custom correlation ID keys. These collide with the
   * SDK's own identifiers (tenant, service, trace/span ids) or provider-specific stamping, so
   * allowing them would corrupt the cross-provider metadata contract. Case-insensitive matching
   * is belt-and-braces given VALID_KEY_PATTERN already rejects uppercase, but implemented anyway
   * to catch any future pattern relaxations.
   */
  private static final Set<String> RESERVED_KEYS =
      Set.of(
          // MDC and span attribute keys (from MultiCloudJLogger)
          MultiCloudJLogger.MDC_TRACE_ID,
          MultiCloudJLogger.MDC_SPAN_ID,
          MultiCloudJLogger.MDC_SDK_SERVICE,
          MultiCloudJLogger.MDC_SDK_PROVIDER,
          MultiCloudJLogger.MDC_TENANT_ID,
          MultiCloudJLogger.MDC_SERVICE_ID,
          MultiCloudJLogger.ATTR_BUCKET,
          // Metadata keys (from SdkLoggingMetadataKeys) for tenant and service.
          // Correlation id defaults are ALLOWED, as explicit restatements of a default.
          SdkLoggingMetadataKeys.SERVICE_ID,
          SdkLoggingMetadataKeys.TENANT_ID);

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
   * Optional custom key name for the correlation ID on all three surfaces: stored object metadata,
   * SLF4J MDC, and OTel span attributes. When {@code null} or blank, the SDK uses its own defaults
   * ({@code sdk-logging-correlation-id} for metadata, {@code correlation_id} for MDC and span).
   * When supplied, this name replaces the defaults everywhere, allowing callers to align the SDK's
   * tracing with their own naming conventions (e.g., {@code x-request-id}, {@code trace-id}).
   *
   * <p>Validation: must match {@code ^[a-z0-9][a-z0-9_-]{0,127}$} and must not collide with
   * reserved keys ({@code bucket}, {@code trace_id}, {@code span_id}, {@code tenant_id}, {@code
   * service_id}, etc.). Explicit restatements of the defaults ({@code correlation_id}, {@code
   * sdk-logging-correlation-id}) are allowed.
   */
  String correlationIdKey;

  /**
   * Hand-written all-args constructor to intercept Lombok's generated {@code build()} call and
   * enforce validation on every construction path, including {@code toBuilder()}.
   *
   * @param correlationId the correlation id value
   * @param tenantId the tenant id
   * @param serviceId the service id
   * @param correlationIdKey the custom correlation id key name (validated)
   * @throws InvalidArgumentException if correlationIdKey violates format or reserved-name rules
   */
  OperationContext(
      String correlationId, String tenantId, String serviceId, String correlationIdKey) {
    this.correlationId = correlationId;
    this.tenantId = tenantId;
    this.serviceId = serviceId;
    this.correlationIdKey = correlationIdKey;

    // Validate correlationIdKey if present (null or blank is valid, means "use defaults")
    if (StringUtils.isNotBlank(correlationIdKey)) {
      if (!VALID_KEY_PATTERN.matcher(correlationIdKey).matches()) {
        // Check common failure modes and give actionable error messages
        if (correlationIdKey.length() > 128) {
          throw new InvalidArgumentException(
              "correlationIdKey exceeds 128 characters: " + correlationIdKey);
        }
        if (!correlationIdKey.equals(correlationIdKey.toLowerCase())) {
          throw new InvalidArgumentException(
              "correlationIdKey must be lowercase (S3 and GCS lowercase user-metadata keys on "
                  + "read, which would cause casing drift). Rejected: "
                  + correlationIdKey
                  + "; try: "
                  + correlationIdKey.toLowerCase());
        }
        if (correlationIdKey.startsWith("-") || correlationIdKey.startsWith("_")) {
          throw new InvalidArgumentException(
              "correlationIdKey must start with an alphanumeric character. Rejected: "
                  + correlationIdKey);
        }
        throw new InvalidArgumentException(
            "correlationIdKey contains invalid characters (allowed: [a-z0-9_-], must start with"
                + " [a-z0-9]). Rejected: "
                + correlationIdKey);
      }

      if (RESERVED_KEYS.contains(correlationIdKey.toLowerCase())) {
        throw new InvalidArgumentException(
            "correlationIdKey collides with reserved SDK identifier. Rejected: "
                + correlationIdKey
                + "; reserved keys: "
                + RESERVED_KEYS);
      }
    }
  }

  /**
   * Resolves the effective correlation ID key for stored object metadata. When a custom {@code
   * correlationIdKey} is supplied, it replaces the default on all three surfaces (metadata, MDC,
   * span). When {@code null} or blank, returns the default metadata key.
   *
   * @return the metadata key under which the correlation id should be stamped
   */
  public String getEffectiveCorrelationIdMetadataKey() {
    return StringUtils.isNotBlank(correlationIdKey)
        ? correlationIdKey
        : SdkLoggingMetadataKeys.CORRELATION_ID;
  }

  /**
   * Resolves the effective correlation ID key for MDC entries and OTel span attributes. When a
   * custom {@code correlationIdKey} is supplied, it replaces the default on all three surfaces
   * (metadata, MDC, span). When {@code null} or blank, returns the default MDC/span key.
   *
   * @return the MDC and span attribute key under which the correlation id should be set
   */
  public String getEffectiveCorrelationIdAttributeKey() {
    return StringUtils.isNotBlank(correlationIdKey)
        ? correlationIdKey
        : MultiCloudJLogger.ATTR_CORRELATION_ID;
  }
}
