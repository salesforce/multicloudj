package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TestIam extends AbstractIam<TestIam> {

    public TestIam(TestIam.Builder builder) {
        super(builder);
    }

    public TestIam() {
        super(new Builder());
    }

    @Override
    protected String createIdentityInProvider(String identityName, String description, String tenantId,
                                              String region, Optional<TrustConfiguration> trustConfig,
                                              Optional<CreateOptions> options) {
        return "mock-identity-id";
    }

    @Override
    protected void attachInlinePolicyToProvider(PolicyDocument policyDocument, String tenantId,
                                                String region, String resource) {
        // Mock implementation
    }

    @Override
    protected String getInlinePolicyDetailsFromProvider(String identityName, String policyName,
                                                        String tenantId, String region) {
        return "mock-policy-details";
    }

    @Override
    protected List<String> getAttachedPoliciesFromProvider(String identityName, String tenantId, String region) {
        return Arrays.asList("policy1", "policy2");
    }

    @Override
    protected void removePolicyFromProvider(String identityName, String policyName, String tenantId,
                                            String region) {
        // Mock implementation
    }

    @Override
    protected void deleteIdentityFromProvider(String identityName, String tenantId, String region) {
        // Mock implementation
    }

    @Override
    protected String getIdentityFromProvider(String identityName, String tenantId, String region) {
        return "mock-identity-arn";
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractIam.Builder<TestIam> {
        protected Builder() {
            providerId("mockProviderId");
        }

        @Override
        public TestIam build() {
            return new TestIam(this);
        }
    }
}

