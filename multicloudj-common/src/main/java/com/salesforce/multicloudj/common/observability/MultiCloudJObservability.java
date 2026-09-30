package com.salesforce.multicloudj.common.observability;

/**
 * Holds process-wide observability defaults for the SDK.
 *
 * <p>The global default tracing policy is consulted when a client is built without an explicit
 * per-client policy. Resolution order is: per-client policy &gt; global default &gt; {@link
 * TracingPolicy#DISABLED}.
 */
public final class MultiCloudJObservability {

  private static volatile TracingPolicy defaultPolicy = TracingPolicy.DISABLED;

  private MultiCloudJObservability() {}

  /**
   * Sets the global default tracing policy. {@code null} is treated as {@link
   * TracingPolicy#DISABLED}.
   *
   * @param policy the global default policy
   */
  public static void setDefaultTracingPolicy(TracingPolicy policy) {
    defaultPolicy = policy != null ? policy : TracingPolicy.DISABLED;
  }

  /**
   * Returns the current global default tracing policy.
   *
   * @return the current global default policy, never {@code null}
   */
  public static TracingPolicy getDefaultTracingPolicy() {
    return defaultPolicy;
  }
}
