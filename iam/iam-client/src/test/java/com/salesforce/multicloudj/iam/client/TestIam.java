package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateIdentityRequest;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.DeleteIdentityRequest;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetIdentityRequest;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.RemovePolicyRequest;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Test implementation of AbstractIam for unit testing.
 * This class provides mock implementations of all IAM operations,
 * returning hardcoded values suitable for testing without requiring
 * actual cloud provider credentials or infrastructure.
 */
public class TestIam extends AbstractIam {

    public TestIam(TestIam.Builder builder) {
        super(builder);
    }

    public TestIam() {
        super(new Builder());
    }

    @Override
    protected String doCreateIdentity(CreateIdentityRequest request) {
        return "mock-identity-id";
    }

    @Override
    protected void doAttachInlinePolicy(AttachInlinePolicyRequest request) {
        // Mock implementation
    }

    @Override
    protected String doGetInlinePolicyDetails(GetInlinePolicyDetailsRequest request) {
        return "mock-policy-details";
    }

    @Override
    protected List<String> doGetAttachedPolicies(GetAttachedPoliciesRequest request) {
        return Arrays.asList("policy1", "policy2");
    }

    @Override
    protected void doRemovePolicy(RemovePolicyRequest request) {
        // Mock implementation
    }

    @Override
    protected void doDeleteIdentity(DeleteIdentityRequest request) {
        // Mock implementation
    }

    @Override
    protected String doGetIdentity(GetIdentityRequest request) {
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

    @Override
    public void close() throws Exception {
        // Mock implementation - no resources to close
    }

    public static class Builder extends AbstractIam.Builder<TestIam, Builder> {
        public Builder() {
            providerId("mockProviderId");
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public TestIam build() {
            return new TestIam(this);
        }
    }
}

