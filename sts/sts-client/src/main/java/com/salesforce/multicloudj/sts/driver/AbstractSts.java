package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import lombok.Getter;

import java.net.URI;

/**
 * Abstract base class for Security Token Service (STS) implementations.
 * This class is internal for SDK and all the providers for STS implementations
 * are supposed to implement it.
 */
public abstract class AbstractSts implements Provider {
    protected final String providerId;
    protected final String region;

    /**
     * Constructs an AbstractSts instance using a Builder.
     * @param builder The Builder instance to use for construction.
     */
    public AbstractSts(Builder<?, ?> builder) {
        this(builder.providerId, builder.region);
    }

    /**
     * Constructs an AbstractSts instance with specified provider ID and region.
     * @param providerId The ID of the provider.
     * @param region The region for the STS.
     */
    public AbstractSts(String providerId, String region) {
        this.providerId = providerId;
        this.region = region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Assumes a role and returns the credentialsOverrider.
     * @param request The AssumedRoleRequest containing role information.
     * @return StsCredentials for the assumed role.
     */
    public StsCredentials assumeRole(AssumedRoleRequest request) {
        return getSTSCredentialsWithAssumeRole(request);
    }

    /**
     * Retrieves the caller identity.
     * @return The CallerIdentity of the current caller.
     */
    public CallerIdentity getCallerIdentity() {
        return getCallerIdentityFromProvider();
    }

    /**
     * Retrieves an access token.
     * @param request The GetAccessTokenRequest containing token request details.
     * @return StsCredentials containing the access token.
     */
    public StsCredentials getAccessToken(GetAccessTokenRequest request) {
        return getAccessTokenFromProvider(request);
    }

    /**
     * Assumes a role with web identity and returns the credentials.
     * @param request The AssumeRoleWithWebIdentityRequest containing role and web identity token information.
     * @return StsCredentials for the assumed role with web identity.
     */
    public StsCredentials assumeRoleWithWebIdentity(AssumeRoleWebIdentityRequest request) {
        return getSTSCredentialsWithAssumeRoleWebIdentity(request);
    }

    /**
     * Abstract builder class for AbstractSts implementations.
     * @param <A> The concrete implementation type of AbstractSts.
     * @param <T> The concrete implementation type of Builder.
     */
    public abstract static class Builder<A extends AbstractSts, T extends Builder<A, T>> implements Provider.Builder {
        @Getter
        protected String region;
        @Getter
        protected URI endpoint;
        protected String providerId;

        /**
         * Sets the region.
         * @param region The region to set.
         * @return This Builder instance.
         */
        public T withRegion(String region) {
            this.region = region;
            return self();
        }

        /**
         * Sets the endpoint to override.
         * @param endpoint The endpoint to set.
         * @return This Builder instance.
         */
        public T withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return self();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public T providerId(String providerId) {
            this.providerId = providerId;
            return self();
        }

        public abstract T self();

        /**
         * Builds and returns an instance of AbstractSts.
         * @return An instance of AbstractSts.
         */
        public abstract A build();
    }

    /**
     * Retrieves STS credentialsOverrider with assumed role.
     * @param request The AssumedRoleRequest.
     * @return StsCredentials for the assumed role.
     */
    protected abstract StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request);

    /**
     * Retrieves the caller identity from the provider.
     * @return The CallerIdentity.
     */
    protected abstract CallerIdentity getCallerIdentityFromProvider();

    /**
     * Retrieves an access token from the provider.
     * @param request The GetAccessTokenRequest.
     * @return StsCredentials containing the access token.
     */
    protected abstract StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request);

    /**
     * Retrieves STS credentials with AssumeRoleWithWebIdentity.
     * @param request The AssumeRoleWithWebIdentityRequest.
     * @return StsCredentials for the assumed role with web identity.
     */
    protected abstract StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(AssumeRoleWebIdentityRequest request);
}