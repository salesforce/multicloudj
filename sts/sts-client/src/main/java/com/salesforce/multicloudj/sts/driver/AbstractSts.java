package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import lombok.Getter;

/**
 * Abstract base class for Security Token Service (STS) implementations. This class is internal for
 * SDK and all the providers for STS implementations are supposed to implement it.
 */
public abstract class AbstractSts implements Provider {
  protected final String providerId;
  protected final String region;

  /**
   * Constructs an AbstractSts instance using a Builder.
   *
   * @param builder The Builder instance to use for construction.
   */
  public AbstractSts(Builder<?, ?> builder) {
    this(builder.providerId, builder.region);
  }

  /**
   * Constructs an AbstractSts instance with specified provider ID and region.
   *
   * @param providerId The ID of the provider.
   * @param region The region for the STS.
   */
  public AbstractSts(String providerId, String region) {
    this.providerId = providerId;
    this.region = region;
  }

  /** {@inheritDoc} */
  @Override
  public String getProviderId() {
    return providerId;
  }

  /**
   * Assumes a role and returns the credentialsOverrider.
   *
   * @param request The AssumedRoleRequest containing role information.
   * @return StsCredentials for the assumed role.
   * @throws IllegalArgumentException if credential scope contains non-storage permissions or
   *     resources
   */
  public StsCredentials assumeRole(AssumedRoleRequest request) {
    validateCredentialScope(request.getCredentialScope());
    return getSTSCredentialsWithAssumeRole(request);
  }

  /**
   * Validates that CredentialScope only contains storage-related permissions and resources.
   *
   * @param credentialScope The CredentialScope to validate (can be null)
   * @throws IllegalArgumentException if scope contains non-storage permissions or resources
   */
  private void validateCredentialScope(CredentialScope credentialScope) {
    if (credentialScope == null) {
      return; // null is valid - no scope restrictions
    }

    for (CredentialScope.ScopeRule rule : credentialScope.getRules()) {
      // Validate resource is storage-only
      String resource = rule.getAvailableResource();
      if (resource != null && !resource.startsWith("storage://")) {
        throw new IllegalArgumentException(
            "Credential scope resource must start with 'storage://'. Found: " + resource);
      }

      // Validate all permissions are storage-only
      for (String permission : rule.getAvailablePermissions()) {
        if (!permission.startsWith("storage:")) {
          throw new IllegalArgumentException(
              "Credential scope permission must start with 'storage:'. Found: " + permission);
        }
      }

      // Validate condition resourcePrefix is storage-only (if present)
      if (rule.getAvailabilityCondition() != null) {
        String resourcePrefix = rule.getAvailabilityCondition().getResourcePrefix();
        if (resourcePrefix != null && !resourcePrefix.startsWith("storage://")) {
          throw new IllegalArgumentException(
              "Credential scope condition resourcePrefix must start with "
                  + "'storage://'. Found: "
                  + resourcePrefix);
        }
      }
    }
  }

  /**
   * Retrieves the caller identity.
   *
   * @param request The GetCallerIdentityRequest.
   * @return The CallerIdentity of the current caller.
   */
  public CallerIdentity getCallerIdentity(GetCallerIdentityRequest request) {
    return getCallerIdentityFromProvider(request);
  }

  /**
   * Retrieves an access token.
   *
   * @param request The GetAccessTokenRequest containing token request details.
   * @return StsCredentials containing the access token.
   */
  public StsCredentials getAccessToken(GetAccessTokenRequest request) {
    return getAccessTokenFromProvider(request);
  }

  /**
   * Assumes a role with web identity and returns the credentials.
   *
   * @param request The AssumeRoleWithWebIdentityRequest containing role and web identity token
   *     information.
   * @return StsCredentials for the assumed role with web identity.
   */
  public StsCredentials assumeRoleWithWebIdentity(AssumeRoleWebIdentityRequest request) {
    return getSTSCredentialsWithAssumeRoleWebIdentity(request);
  }

  /**
   * Abstract builder class for AbstractSts implementations.
   *
   * @param <A> The concrete implementation type of AbstractSts.
   * @param <T> The concrete implementation type of Builder.
   */
  public abstract static class Builder<A extends AbstractSts, T extends Builder<A, T>>
      implements Provider.Builder {
    @Getter protected String region;
    @Getter protected URI endpoint;
    @Getter protected URI proxyEndpoint;
    @Getter protected Boolean useSystemPropertyProxyValues;
    @Getter protected Boolean useEnvironmentVariableProxyValues;
    @Getter protected RetryConfig retryConfig;
    protected String providerId;

    /**
     * Sets the region.
     *
     * @param region The region to set.
     * @return This Builder instance.
     */
    public T withRegion(String region) {
      this.region = region;
      return self();
    }

    /**
     * Sets the endpoint to override.
     *
     * @param endpoint The endpoint to set.
     * @return This Builder instance.
     */
    public T withEndpoint(URI endpoint) {
      this.endpoint = endpoint;
      return self();
    }

    /**
     * Sets the proxy endpoint to override.
     *
     * @param proxyEndpoint The proxy endpoint to set.
     * @return This Builder instance.
     */
    public T withProxyEndpoint(URI proxyEndpoint) {
      this.proxyEndpoint = proxyEndpoint;
      return self();
    }

    /**
     * Method to control whether system property values (e.g., http.proxyHost, http.proxyPort,
     * https.proxyHost, https.proxyPort) should be used for proxy configuration. When set to false,
     * these system properties will be ignored.
     *
     * @param useSystemPropertyProxyValues Whether to use system property values for proxy
     *     configuration
     * @return This Builder instance.
     */
    public T withUseSystemPropertyProxyValues(Boolean useSystemPropertyProxyValues) {
      this.useSystemPropertyProxyValues = useSystemPropertyProxyValues;
      return self();
    }

    /**
     * Method to control whether environment variable values (e.g., HTTP_PROXY, HTTPS_PROXY,
     * NO_PROXY) should be used for proxy configuration. When set to false, these environment
     * variables will be ignored.
     *
     * @param useEnvironmentVariableProxyValues Whether to use environment variable values for proxy
     *     configuration
     * @return This Builder instance.
     */
    public T withUseEnvironmentVariableProxyValues(
        Boolean useEnvironmentVariableProxyValues) {
      this.useEnvironmentVariableProxyValues = useEnvironmentVariableProxyValues;
      return self();
    }

    /**
     * Sets the retry configuration applied to every request the STS client makes. When not set,
     * the provider's default retry behavior is used.
     *
     * @param retryConfig The retry configuration, or null to use the provider's defaults.
     * @return This Builder instance.
     * @throws InvalidArgumentException if the configuration has non-positive attempts, delays, or
     *     timeouts for the selected mode.
     */
    public T withRetryConfig(RetryConfig retryConfig) {
      validateRetryConfig(retryConfig);
      this.retryConfig = retryConfig;
      return self();
    }

    private static void validateRetryConfig(RetryConfig retryConfig) {
      if (retryConfig == null) {
        return;
      }
      if (retryConfig.getMaxAttempts() != null && retryConfig.getMaxAttempts() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.maxAttempts must be greater than 0, got: " + retryConfig.getMaxAttempts());
      }
      if (retryConfig.getMode() == RetryConfig.Mode.EXPONENTIAL) {
        if (retryConfig.getInitialDelayMillis() <= 0) {
          throw new InvalidArgumentException(
              "RetryConfig.initialDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                  + retryConfig.getInitialDelayMillis());
        }
        if (retryConfig.getMaxDelayMillis() <= 0) {
          throw new InvalidArgumentException(
              "RetryConfig.maxDelayMillis must be greater than 0 for EXPONENTIAL mode, got: "
                  + retryConfig.getMaxDelayMillis());
        }
      } else if (retryConfig.getMode() == RetryConfig.Mode.FIXED
          && retryConfig.getFixedDelayMillis() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.fixedDelayMillis must be greater than 0 for FIXED mode, got: "
                + retryConfig.getFixedDelayMillis());
      }
      if (retryConfig.getAttemptTimeout() != null && retryConfig.getAttemptTimeout() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.attemptTimeout must be greater than 0, got: "
                + retryConfig.getAttemptTimeout());
      }
      if (retryConfig.getTotalTimeout() != null && retryConfig.getTotalTimeout() <= 0) {
        throw new InvalidArgumentException(
            "RetryConfig.totalTimeout must be greater than 0, got: "
                + retryConfig.getTotalTimeout());
      }
    }

    /** {@inheritDoc} */
    @Override
    public T providerId(String providerId) {
      this.providerId = providerId;
      return self();
    }

    /**
     * Returns the builder instance.
     *
     * @return This Builder instance.
     */
    public abstract T self();

    /**
     * Builds and returns an instance of AbstractSts.
     *
     * @return An instance of AbstractSts.
     */
    public abstract A build();
  }

  /**
   * Retrieves STS credentialsOverrider with assumed role.
   *
   * @param request The AssumedRoleRequest.
   * @return StsCredentials for the assumed role.
   */
  protected abstract StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request);

  /**
   * Retrieves the caller identity from the provider.
   *
   * @param request The GetCallerIdentityRequest.
   * @return The CallerIdentity.
   */
  protected abstract CallerIdentity getCallerIdentityFromProvider(GetCallerIdentityRequest request);

  /**
   * Retrieves an access token from the provider.
   *
   * @param request The GetAccessTokenRequest.
   * @return StsCredentials containing the access token.
   */
  protected abstract StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request);

  /**
   * Retrieves STS credentials with AssumeRoleWithWebIdentity.
   *
   * @param request The AssumeRoleWithWebIdentityRequest.
   * @return StsCredentials for the assumed role with web identity.
   */
  protected abstract StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(
      AssumeRoleWebIdentityRequest request);
}
