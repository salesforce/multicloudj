package com.salesforce.multicloudj.sts.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.net.URI;
import java.util.ServiceLoader;

/**
 * StsVerifier provides portable validation of cloud native auth signed requests. It is the
 * receiving half of the signing flow performed by {@link StsUtilities}: given the portable {@code
 * signedIdentity} string a caller produced, it proves that identity against the substrate and
 * returns the verified {@link CallerIdentity}.
 */
public class StsVerifier {
  protected AbstractStsVerifier verifier;

  /**
   * Constructor for StsVerifier with StsVerifierBuilder.
   *
   * @param builder The StsVerifierBuilder used to construct this verifier.
   */
  protected StsVerifier(StsVerifierBuilder builder) {
    this.verifier = builder.verifier;
  }

  /**
   * Creates a new StsVerifierBuilder for the specified provider.
   *
   * @param providerId The ID of the provider/substrate such as aws.
   * @return A new StsVerifierBuilder instance.
   */
  public static StsVerifierBuilder builder(String providerId) {
    return new StsVerifierBuilder(providerId);
  }

  /**
   * Returns an Iterable of all available AbstractStsVerifier implementations.
   *
   * @return An Iterable of AbstractStsVerifier instances.
   */
  private static Iterable<AbstractStsVerifier> all() {
    ServiceLoader<AbstractStsVerifier> services = ServiceLoader.load(AbstractStsVerifier.class);
    ImmutableSet.Builder<AbstractStsVerifier> builder = ImmutableSet.builder();
    for (AbstractStsVerifier service : services) {
      builder.add(service);
    }
    return builder.build();
  }

  /**
   * Finds the builder for the specified provider.
   *
   * @param providerId The ID of the provider.
   * @return The AbstractStsVerifier.Builder for the specified provider.
   * @throws IllegalArgumentException if no provider is found for the given ID.
   */
  private static AbstractStsVerifier.Builder<?, ?> findProviderBuilder(String providerId) {
    for (AbstractStsVerifier provider : all()) {
      if (provider.getProviderId().equals(providerId)) {
        return createBuilderInstance(provider);
      }
    }
    throw new IllegalArgumentException(
        "No STS verifier provider found for providerId: " + providerId);
  }

  /**
   * Creates a builder instance for the given provider.
   *
   * @param provider The AbstractStsVerifier provider.
   * @return The AbstractStsVerifier.Builder for the provider.
   * @throws RuntimeException if the builder creation fails.
   */
  private static AbstractStsVerifier.Builder<?, ?> createBuilderInstance(
      AbstractStsVerifier provider) {
    try {
      return (AbstractStsVerifier.Builder<?, ?>)
          provider.getClass().getMethod("builder").invoke(provider);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create builder for provider: " + provider.getClass().getName(), e);
    }
  }

  /**
   * Validates a signed auth request and returns the identity of the original signer.
   *
   * @param signedIdentity the portable token string produced by {@link
   *     StsUtilities#newCloudNativeAuthSignedRequest}.
   * @return the verified {@link CallerIdentity}.
   */
  public CallerIdentity validateSignedAuthRequest(String signedIdentity) {
    return validateSignedAuthRequest(signedIdentity, null);
  }

  /**
   * Validates a signed auth request using the supplied options and returns the identity of the
   * original signer.
   *
   * @param signedIdentity the portable token string produced by {@link
   *     StsUtilities#newCloudNativeAuthSignedRequest}.
   * @param options validation options such as expected custom headers; may be null for defaults.
   * @return the verified {@link CallerIdentity}.
   */
  public CallerIdentity validateSignedAuthRequest(String signedIdentity, ValidateOptions options) {
    try {
      return this.verifier.verifySignedAuthRequest(signedIdentity, options);
    } catch (Throwable t) {
      throw this.verifier.mapException(t);
    }
  }

  /** Builder class for StsVerifier. */
  public static class StsVerifierBuilder {
    protected AbstractStsVerifier verifier;
    protected AbstractStsVerifier.Builder<?, ?> builder;

    /**
     * Constructor for StsVerifierBuilder.
     *
     * @param providerId The ID of the provider such as aws.
     */
    public StsVerifierBuilder(String providerId) {
      this.builder = findProviderBuilder(providerId);
    }

    /**
     * Sets the region for the STS verifier.
     *
     * @param region The region to set.
     * @return This StsVerifierBuilder instance.
     */
    public StsVerifierBuilder withRegion(String region) {
      this.builder.withRegion(region);
      return this;
    }

    /**
     * Sets the endpoint override for the STS verifier.
     *
     * @param endpoint The endpoint to set.
     * @return This StsVerifierBuilder instance.
     */
    public StsVerifierBuilder withEndpoint(URI endpoint) {
      this.builder.withEndpoint(endpoint);
      return this;
    }

    /**
     * Sets the proxy endpoint override for the STS verifier.
     *
     * @param proxyEndpoint The proxy endpoint to set.
     * @return This StsVerifierBuilder instance.
     */
    public StsVerifierBuilder withProxyEndpoint(URI proxyEndpoint) {
      this.builder.withProxyEndpoint(proxyEndpoint);
      return this;
    }

    /**
     * Controls whether system property proxy values are used.
     *
     * @param useSystemPropertyProxyValues Whether to use system property values for proxy
     *     configuration.
     * @return This StsVerifierBuilder instance.
     */
    public StsVerifierBuilder withUseSystemPropertyProxyValues(
        Boolean useSystemPropertyProxyValues) {
      this.builder.withUseSystemPropertyProxyValues(useSystemPropertyProxyValues);
      return this;
    }

    /**
     * Controls whether environment variable proxy values are used.
     *
     * @param useEnvironmentVariableProxyValues Whether to use environment variable values for proxy
     *     configuration.
     * @return This StsVerifierBuilder instance.
     */
    public StsVerifierBuilder withUseEnvironmentVariableProxyValues(
        Boolean useEnvironmentVariableProxyValues) {
      this.builder.withUseEnvironmentVariableProxyValues(useEnvironmentVariableProxyValues);
      return this;
    }

    /**
     * Builds and returns an StsVerifier instance.
     *
     * @return A new StsVerifier instance.
     */
    public StsVerifier build() {
      this.verifier = this.builder.build();
      return new StsVerifier(this);
    }
  }
}
