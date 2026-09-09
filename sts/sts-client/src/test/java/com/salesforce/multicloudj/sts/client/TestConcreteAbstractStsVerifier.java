package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;

/** Test double for the STS verifier facade and abstract driver contract. */
public class TestConcreteAbstractStsVerifier extends AbstractStsVerifier {

  public TestConcreteAbstractStsVerifier(Builder builder) {
    super(builder);
  }

  public TestConcreteAbstractStsVerifier() {
    super(new Builder());
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  protected CallerIdentity validateSignedAuthRequest(
      String signedIdentity, ValidateOptions options) {
    if ("boom".equals(signedIdentity)) {
      throw new IllegalStateException("boom");
    }
    String headerCount =
        options == null ? "0" : String.valueOf(options.getExpectedCustomHeaders().size());
    return new CallerIdentity(signedIdentity, "arn:test:" + signedIdentity, headerCount);
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    return new UnknownException(t);
  }

  public static class Builder
      extends AbstractStsVerifier.Builder<TestConcreteAbstractStsVerifier, Builder> {
    protected Builder() {
      providerId("mockProviderId");
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public TestConcreteAbstractStsVerifier build() {
      return new TestConcreteAbstractStsVerifier(this);
    }
  }
}
