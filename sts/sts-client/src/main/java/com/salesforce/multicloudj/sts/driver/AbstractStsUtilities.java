package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.SignOptions;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import java.net.http.HttpRequest;
import lombok.Getter;

public abstract class AbstractStsUtilities<T extends AbstractStsUtilities<T>> implements Provider {
  protected final String providerId;
  protected final String region;
  protected final CredentialsOverrider credentialsOverrider;

  public AbstractStsUtilities(AbstractStsUtilities.Builder<T> builder) {
    this.providerId = builder.providerId;
    this.region = builder.region;
    this.credentialsOverrider = builder.credentialsOverrider;
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
