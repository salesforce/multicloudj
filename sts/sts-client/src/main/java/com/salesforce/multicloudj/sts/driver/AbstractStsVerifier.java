package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import lombok.Getter;

/**
 * Abstract base class for Security Token Service (STS) verifier implementations. A verifier is the
 * receiving half of the cloud native auth flow: it consumes the portable {@code signedIdentity}
 * string produced by an {@link AbstractStsUtilities} signer, proves it against the substrate, and
 * returns the identity of the original signer.
 *
 * <p>This class is internal to the SDK; every provider that supports verification implements it.
 */
public abstract class AbstractStsVerifier implements Provider {
  protected final String providerId;
  protected final String region;

  /**
   * Constructs an AbstractStsVerifier from a Builder.
   *
   * @param builder The Builder instance to use for construction.
   */
  public AbstractStsVerifier(Builder<?, ?> builder) {
    this.providerId = builder.providerId;
    this.region = builder.region;
  }

  /** {@inheritDoc} */
  @Override
  public String getProviderId() {
    return providerId;
  }

  /**
   * Validates a signed auth request and returns the identity of the original signer.
   *
   * @param signedIdentity the portable token string produced by a signer's {@code
   *     newCloudNativeAuthSignedRequest}. For AWS and Alibaba this is a presigned GetCallerIdentity
   *     URL; for GCP it is a signed JWT.
   * @return the verified {@link CallerIdentity} of the signer.
   */
  public CallerIdentity verifySignedAuthRequest(String signedIdentity) {
    return verifySignedAuthRequest(signedIdentity, null);
  }

  /**
   * Validates a signed auth request using the supplied options and returns the identity of the
   * original signer.
   *
   * @param signedIdentity the portable token string produced by a signer's {@code
   *     newCloudNativeAuthSignedRequest}.
   * @param options validation options such as expected custom headers; may be null for defaults.
   * @return the verified {@link CallerIdentity} of the signer.
   */
  public CallerIdentity verifySignedAuthRequest(String signedIdentity, ValidateOptions options) {
    return validateSignedAuthRequest(signedIdentity, options);
  }

  /**
   * Abstract builder for AbstractStsVerifier implementations.
   *
   * @param <A> The concrete implementation type of AbstractStsVerifier.
   * @param <T> The concrete implementation type of Builder.
   */
  public abstract static class Builder<A extends AbstractStsVerifier, T extends Builder<A, T>>
      implements Provider.Builder {
    @Getter protected String region;
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
     * Builds and returns an instance of AbstractStsVerifier.
     *
     * @return An instance of AbstractStsVerifier.
     */
    public abstract A build();
  }

  /**
   * Substrate-specific implementation that validates the signed identity and returns the caller
   * identity.
   *
   * @param signedIdentity the portable token string produced by a signer.
   * @param options validation options; may be null.
   * @return the verified {@link CallerIdentity}.
   */
  protected abstract CallerIdentity validateSignedAuthRequest(
      String signedIdentity, ValidateOptions options);
}
