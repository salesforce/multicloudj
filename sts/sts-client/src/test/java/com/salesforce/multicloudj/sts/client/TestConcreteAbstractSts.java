package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

public class TestConcreteAbstractSts extends AbstractSts<TestConcreteAbstractSts> {

    public TestConcreteAbstractSts(TestConcreteAbstractSts.Builder builder) {
        super(builder);
    }

    public TestConcreteAbstractSts() {
        super(new Builder());
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
        return null;
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider() {
        return null;
    }

    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        return null;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractSts.Builder<TestConcreteAbstractSts> {
        protected Builder() {
            providerId("mockProviderId");
        }

        @Override
        public TestConcreteAbstractSts build() {
            return new TestConcreteAbstractSts(this);
        }
    }
}
