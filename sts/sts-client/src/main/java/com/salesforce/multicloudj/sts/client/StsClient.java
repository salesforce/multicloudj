package com.salesforce.multicloudj.sts.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerConfig;
import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerExecutor;
import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * StsClient class in the Portable Client for interacting with Security Token Service (STS) in a
 * substrate agnostic way.
 */
public class StsClient implements AutoCloseable {
  protected AbstractSts sts;

  /**
   * Optional circuit breaker guarding every provider call. Null unless a circuit-breaker
   * configuration was supplied to the builder, in which case behavior is byte-for-byte identical to
   * a client without a breaker.
   */
  private final CircuitBreakerExecutor circuitBreakerExecutor;

  /**
   * Constructor for StsClient with StsBuilder.
   *
   * @param sts The abstract used to back this client for implementation.
   */
  protected StsClient(AbstractSts sts) {
    this(sts, null);
  }

  /**
   * Constructor for StsClient with an optional circuit breaker.
   *
   * @param sts The abstract used to back this client for implementation.
   * @param circuitBreakerExecutor The circuit breaker to guard provider calls, or null to disable.
   */
  protected StsClient(AbstractSts sts, CircuitBreakerExecutor circuitBreakerExecutor) {
    this.sts = sts;
    this.circuitBreakerExecutor = circuitBreakerExecutor;
  }

  /**
   * Single seam through which every provider operation runs. The provider call and its exception
   * mapping ({@link AbstractSts#mapException(Throwable)}) happen inside the guarded supplier, so
   * the breaker observes the mapped {@link SubstrateSdkException} and its retryability. When no
   * breaker is configured, the supplier is invoked directly — behavior is identical to the
   * pre-breaker client.
   *
   * @param operation the raw provider call
   * @param <T> the operation's result type
   * @return the operation's result
   */
  private <T> T call(Supplier<T> operation) {
    Supplier<T> mapped =
        () -> {
          try {
            return operation.get();
          } catch (Throwable t) {
            throw this.sts.mapException(t);
          }
        };
    if (circuitBreakerExecutor == null) {
      return mapped.get();
    }
    return circuitBreakerExecutor.execute(mapped);
  }

  /**
   * Creates a new StsBuilder for the specified provider.
   *
   * @param providerId The ID of the provider/substrate such as aws.
   * @return A new StsBuilder instance.
   */
  public static StsBuilder builder(String providerId) {
    return new StsBuilder(providerId);
  }

  /**
   * Returns an Iterable of all available AbstractSts implementations.
   *
   * @return An Iterable of AbstractSts instances.
   */
  private static Iterable<AbstractSts> all() {
    ServiceLoader<AbstractSts> services = ServiceLoader.load(AbstractSts.class);
    ImmutableSet.Builder<AbstractSts> builder = ImmutableSet.builder();
    for (AbstractSts service : services) {
      builder.add(service);
    }
    return builder.build();
  }

  /**
   * Finds the builder for the specified provider.
   *
   * @param providerId The ID of the provider.
   * @return The AbstractSts.Builder for the specified provider.
   * @throws IllegalArgumentException if no provider is found for the given ID.
   */
  private static AbstractSts.Builder<?, ?> findProviderBuilder(String providerId) {
    for (AbstractSts provider : all()) {
      if (provider.getProviderId().equals(providerId)) {
        return createBuilderInstance(provider);
      }
    }
    throw new IllegalArgumentException(
        "No cloud storage provider found for providerId: " + providerId);
  }

  /**
   * Creates a builder instance for the given provider.
   *
   * @param provider The AbstractSts provider.
   * @return The AbstractSts.Builder for the provider.
   * @throws RuntimeException if the builder creation fails.
   */
  private static AbstractSts.Builder<?, ?> createBuilderInstance(AbstractSts provider) {
    try {
      return (AbstractSts.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create builder for provider: " + provider.getClass().getName(), e);
    }
  }

  /**
   * Assumes a role and returns the temporary credentialsOverrider for that role.
   *
   * @param request The AssumedRoleRequest.
   * @return The StsCredentials for the assumed role.
   */
  public StsCredentials getAssumeRoleCredentials(AssumedRoleRequest request) {
    return call(() -> this.sts.assumeRole(request));
  }

  /**
   * Gets the caller identity for the default credentialsOverrider.
   *
   * @return The CallerIdentity.
   */
  public CallerIdentity getCallerIdentity() {
    return getCallerIdentity(GetCallerIdentityRequest.builder().build());
  }

  /**
   * Gets the caller identity for the default credentialsOverrider.
   *
   * @return The CallerIdentity.
   */
  public CallerIdentity getCallerIdentity(GetCallerIdentityRequest request) {
    return call(() -> this.sts.getCallerIdentity(request));
  }

  /**
   * Gets an access token for the default credentialsOverrider.
   *
   * @param request The GetAccessTokenRequest.
   * @return The StsCredentials containing the access token.
   */
  public StsCredentials getAccessToken(GetAccessTokenRequest request) {
    return call(() -> this.sts.getAccessToken(request));
  }

  /**
   * Assumes a role with web identity and returns the temporary credentials for that role.
   *
   * @param request The AssumeRoleWithWebIdentityRequest.
   * @return The StsCredentials for the assumed role with web identity.
   */
  public StsCredentials getAssumeRoleWithWebIdentityCredentials(
      AssumeRoleWebIdentityRequest request) {
    return call(() -> this.sts.assumeRoleWithWebIdentity(request));
  }

  /** Closes the underlying STS provider client and releases any resources it holds. */
  @Override
  public void close() throws Exception {
    if (this.sts != null) {
      this.sts.close();
    }
  }

  /** Builder class for StsClient. */
  public static class StsBuilder {
    protected String region;
    protected URI endpoint;
    protected AbstractSts sts;
    protected AbstractSts.Builder<?, ?> stsBuilder;
    protected CircuitBreakerConfig circuitBreakerConfig;

    /**
     * Constructor for StsBuilder.
     *
     * @param providerId The ID of the provider such as aws.
     */
    public StsBuilder(String providerId) {
      this.stsBuilder = findProviderBuilder(providerId);
    }

    /**
     * Sets the region for the STS client.
     *
     * @param region The region to set.
     * @return This StsBuilder instance.
     */
    public StsBuilder withRegion(String region) {
      this.region = region;
      this.stsBuilder.withRegion(region);
      return this;
    }

    /**
     * Sets the endpoint to override for the STS client.
     *
     * @param endpoint The endpoint to set.
     * @return This StsBuilder instance.
     */
    public StsBuilder withEndpoint(URI endpoint) {
      this.endpoint = endpoint;
      this.stsBuilder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Sets the proxy endpoint to override for the STS client.
     *
     * @param proxyEndpoint The proxy endpoint to set.
     * @return This StsBuilder instance.
     */
    public StsBuilder withProxyEndpoint(URI proxyEndpoint) {
      this.stsBuilder.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    /**
     * Sets whether to use system property values for proxy configuration.
     *
     * @param useSystemPropertyProxyValues Whether to use system property values for proxy
     *     configuration
     * @return This StsBuilder instance.
     */
    public StsBuilder withUseSystemPropertyProxyValues(Boolean useSystemPropertyProxyValues) {
      this.stsBuilder.withUseSystemPropertyProxyValues(useSystemPropertyProxyValues);
      return this;
    }

    /**
     * Sets whether to use environment variable values for proxy configuration.
     *
     * @param useEnvironmentVariableProxyValues Whether to use environment variable values for proxy
     *     configuration
     * @return This StsBuilder instance.
     */
    public StsBuilder withUseEnvironmentVariableProxyValues(
        Boolean useEnvironmentVariableProxyValues) {
      this.stsBuilder.withUseEnvironmentVariableProxyValues(useEnvironmentVariableProxyValues);
      return this;
    }

    /**
     * Enables a circuit breaker that guards every provider call made by the built client. When this
     * is not called (or is passed null), the client behaves exactly as before — no breaker is
     * created and calls run straight through.
     *
     * <p>The breaker counts only retryable {@link SubstrateSdkException}s as failures, so caller
     * errors (e.g. invalid arguments) never trip it. Once open, calls are rejected with a
     * non-retryable {@link CircuitBreakerOpenException} until the breaker's wait duration elapses.
     * See {@link CircuitBreakerConfig} for tuning guidance.
     *
     * @param circuitBreakerConfig the breaker configuration, or null to leave the breaker disabled
     * @return This StsBuilder instance.
     */
    public StsBuilder withCircuitBreakerConfig(CircuitBreakerConfig circuitBreakerConfig) {
      this.circuitBreakerConfig = circuitBreakerConfig;
      return this;
    }

    /**
     * Builds and returns an StsClient instance.
     *
     * @return A new StsClient instance.
     */
    public StsClient build() {
      this.sts = this.stsBuilder.build();
      CircuitBreakerExecutor executor =
          circuitBreakerConfig == null
              ? null
              : new CircuitBreakerExecutor("sts", circuitBreakerConfig);
      return new StsClient(this.sts, executor);
    }
  }
}
