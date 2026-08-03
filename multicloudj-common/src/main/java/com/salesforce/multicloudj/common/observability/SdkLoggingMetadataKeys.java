package com.salesforce.multicloudj.common.observability;

/**
 * Object-metadata keys under which the SDK persists observability identifiers from the {@link
 * OperationContext} onto a stored object during upload.
 *
 * <p>These keys form a cross-provider wire contract: the value stamped by one provider must be
 * readable under the same key regardless of which cloud stored the object, so cloud audit logs can
 * be traced back to the originating request, tenant, and service. They are defined once here so the
 * providers that stamp them cannot drift apart.
 */
public final class SdkLoggingMetadataKeys {

  /**
   * Metadata key under which the SDK persists the operation correlation id, so the stored value
   * matches the correlation id that appears in the same upload's logs and trace span.
   */
  public static final String CORRELATION_ID = "sdk-logging-correlation-id";

  /**
   * Metadata key under which the SDK persists the operation service id, so the calling service can
   * be traced from the object's cloud audit logs.
   */
  public static final String SERVICE_ID = "sdk-logging-service-id";

  /**
   * Metadata key under which the SDK persists the operation tenant id, so the tenant can be traced
   * from the object's cloud audit logs.
   */
  public static final String TENANT_ID = "sdk-logging-tenant-id";

  private SdkLoggingMetadataKeys() {}
}
