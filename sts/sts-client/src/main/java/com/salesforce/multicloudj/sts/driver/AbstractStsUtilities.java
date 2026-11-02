package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import java.net.URI;
import java.net.http.HttpRequest;
import lombok.Getter;

public abstract class AbstractStsUtilities<T extends AbstractStsUtilities<T>> implements Provider {
  protected final String providerId;
  protected final String region;
  protected final CredentialsOverrider credentialsOverrider;
  protected final URI endpoint;

  public AbstractStsUtilities(AbstractStsUtilities.Builder<T> builder) {
    this.providerId = builder.providerId;
    this.region = builder.region;
    this.credentialsOverrider = builder.credentialsOverrider;
    this.endpoint = builder.endpoint;
  }

  /** {@inheritDoc} */
  @Override
  public String getProviderId() {
    return providerId;
  }

  /**
   * signs a passed in request and returns
   *
   * @param request The HttpRequest containing the request details.
   * @return SignedAuthRequest
   */
  public SignedAuthRequest cloudNativeAuthSignedRequest(HttpRequest request) {
    return newCloudNativeAuthSignedRequest(request);
  }

  /**
   * signs a passed in request using the supplied options and returns
   *
   * @param request The HttpRequest containing the request details. May be null when the caller only
   *     needs a signed STS request without a service request to hash.
   * @param options The SignOptions controlling custom headers and header exclusions.
   * @return SignedAuthRequest
   */
  public SignedAuthRequest cloudNativeAuthSignedRequest(
      HttpRequest request, SignOptions options) {
    return newCloudNativeAuthSignedRequest(request, options);
  }

  public abstract static class Builder<T extends AbstractStsUtilities<T>>
      implements Provider.Builder {
    @Getter
    protected String providerId;
    protected String region;
    protected CredentialsOverrider credentialsOverrider;
    protected URI endpoint;

    /**
     * Method to supply region
     *
     * @param region Region
     * @return An instance of self
     */
    public AbstractStsUtilities.Builder withRegion(String region) {
      this.region = region;
      return this;
    }

    /**
     * Method to supply credentialsOverrider
     *
     * @param credentialsOverrider Credentials overrider
     * @return An instance of self
     */
    public AbstractStsUtilities.Builder withCredentialsOverrider(
        CredentialsOverrider credentialsOverrider) {
      this.credentialsOverrider = credentialsOverrider;
      return this;
    }

    /**
     * Sets the endpoint the provider uses to produce the signed identity.
     *
     * @param endpoint The absolute https endpoint to set: a host and optional port, with no path
     *     other than {@code /}.
     * @return An instance of self
     * @throws InvalidArgumentException if the endpoint is null, not absolute, has no host, carries
     *     user info, uses a scheme other than https, or has a path, query, or fragment.
     */
    public AbstractStsUtilities.Builder withEndpoint(URI endpoint) {
      validateEndpoint(endpoint);
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Checks the rules every provider shares. Only https is accepted: the verifiers reject signed
     * identities for any other scheme, and the signed request carries credentials. No provider can
     * carry an endpoint path: the SigV4
     * canonical URI is fixed at {@code /}, and the other providers take a bare host. A query or
     * fragment would not be covered by the signature.
     */
    private static void validateEndpoint(URI endpoint) {
      if (endpoint == null || !endpoint.isAbsolute() || endpoint.getHost() == null) {
        throw new InvalidArgumentException("endpoint must be an absolute URI with a host");
      }
      if (!"https".equalsIgnoreCase(endpoint.getScheme())) {
        throw new InvalidArgumentException("endpoint scheme must be https: " + endpoint);
      }
      // User info in an endpoint would leak credentials into logs and request URLs.
      if (endpoint.getRawUserInfo() != null) {
        throw new InvalidArgumentException("endpoint must not contain user info");
      }
      String path = endpoint.getRawPath();
      if ((path != null && !path.isEmpty() && !"/".equals(path))
          || endpoint.getRawQuery() != null
          || endpoint.getRawFragment() != null) {
        throw new InvalidArgumentException(
            "endpoint must not have a path, query, or fragment: " + endpoint);
      }
    }

    @Override
    public Builder<T> providerId(String providerId) {
      this.providerId = providerId;
      return this;
    }

    public abstract T build();
  }

  // Abstract methods for substrate-specific implementations
  protected abstract SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request);

  /**
   * Substrate-specific implementation that honors the supplied signing options. The default
   * implementation ignores the options and delegates to {@link
   * #newCloudNativeAuthSignedRequest(HttpRequest)}; providers that support the options override
   * this method.
   *
   * @param request The HttpRequest containing the request details. May be null.
   * @param options The SignOptions controlling custom headers and header exclusions.
   * @return SignedAuthRequest
   */
  protected SignedAuthRequest newCloudNativeAuthSignedRequest(
      HttpRequest request, SignOptions options) {
    return newCloudNativeAuthSignedRequest(request);
  }
}
