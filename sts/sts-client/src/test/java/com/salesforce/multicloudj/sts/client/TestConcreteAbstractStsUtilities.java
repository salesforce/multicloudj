package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;

import java.net.http.HttpRequest;

public class TestConcreteAbstractStsUtilities extends AbstractStsUtilities<TestConcreteAbstractStsUtilities> {

    public TestConcreteAbstractStsUtilities(TestConcreteAbstractStsUtilities.Builder builder) {
        super(builder);
    }

    public TestConcreteAbstractStsUtilities() {
        super(new Builder());
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public String getProviderId() {
        return this.providerId;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
        return new SignedAuthRequest(request, credentialsOverrider.getSessionCredentials());
    }

    public static class Builder extends AbstractStsUtilities.Builder<TestConcreteAbstractStsUtilities> {
        protected Builder() {
            providerId("mockProviderId");
        }

        @Override
        public TestConcreteAbstractStsUtilities build() {
            return new TestConcreteAbstractStsUtilities(this);
        }
    }
}
