package com.salesforce.multicloudj.sts.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.net.URI;
import java.util.ServiceLoader;

/**
 * StsClient class in the Portable Client for interacting with Security Token Service (STS)
 * in a substrate agnostic way.
 */
public class StsClient {
    protected AbstractSts sts;

    /**
     * Constructor for StsClient with StsBuilder.
     * @param sts The abstract used to back this client for implementation.
     */
    protected StsClient(AbstractSts sts) {
        this.sts = sts;
    }

    /**
     * Creates a new StsBuilder for the specified provider.
     * @param providerId The ID of the provider/substrate such as aws.
     * @return A new StsBuilder instance.
     */
    public static StsBuilder builder(String providerId) {
        return new StsBuilder(providerId);
    }

    /**
     * Returns an Iterable of all available AbstractSts implementations.
     * @return An Iterable of AbstractSts instances.
     */
    private static Iterable<AbstractSts> all() {
        ServiceLoader<AbstractSts> services = ServiceLoader.load(AbstractSts.class);
        ImmutableSet.Builder<AbstractSts> builder = ImmutableSet.builder();
        for (AbstractSts service : services) {
            builder.add(service);
        }
        return builder.build();
    }

    /**
     * Finds the builder for the specified provider.
     * @param providerId The ID of the provider.
     * @return The AbstractSts.Builder for the specified provider.
     * @throws IllegalArgumentException if no provider is found for the given ID.
     */
    private static AbstractSts.Builder<?, ?> findProviderBuilder(String providerId) {
        for (AbstractSts provider : all()) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No cloud storage provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     * @param provider The AbstractSts provider.
     * @return The AbstractSts.Builder for the provider.
     * @throws RuntimeException if the builder creation fails.
     */
    private static AbstractSts.Builder<?, ?> createBuilderInstance(AbstractSts provider) {
        try {
            return (AbstractSts.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }

    /**
     * Assumes a role and returns the temporary credentialsOverrider for that role.
     * @param request The AssumedRoleRequest.
     * @return The StsCredentials for the assumed role.
     */
    public StsCredentials getAssumeRoleCredentials(AssumedRoleRequest request) {
        try {
            return this.sts.assumeRole(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.sts.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Gets the caller identity for the default credentialsOverrider.
     * @return The CallerIdentity.
     */
    public CallerIdentity getCallerIdentity() {
        try {
            return getCallerIdentity(GetCallerIdentityRequest.builder().build());
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.sts.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Gets the caller identity for the default credentialsOverrider.
     * @return The CallerIdentity.
     */
    public CallerIdentity getCallerIdentity(GetCallerIdentityRequest request) {
        try {
            return this.sts.getCallerIdentity(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.sts.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Gets an access token for the default credentialsOverrider.
     * @param request The GetAccessTokenRequest.
     * @return The StsCredentials containing the access token.
     */
    public StsCredentials getAccessToken(GetAccessTokenRequest request) {
        try {
            return this.sts.getAccessToken(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.sts.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Assumes a role with web identity and returns the temporary credentials for that role.
     * @param request The AssumeRoleWithWebIdentityRequest.
     * @return The StsCredentials for the assumed role with web identity.
     */
    public StsCredentials getAssumeRoleWithWebIdentityCredentials(AssumeRoleWebIdentityRequest request) {
        try {
            return this.sts.assumeRoleWithWebIdentity(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.sts.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Builder class for StsClient.
     */
    public static class StsBuilder {
        protected String region;
        protected URI endpoint;
        protected AbstractSts sts;
        protected AbstractSts.Builder<?, ?> stsBuilder;

        /**
         * Constructor for StsBuilder.
         * @param providerId The ID of the provider such as aws.
         */
        public StsBuilder(String providerId) {
            this.stsBuilder = findProviderBuilder(providerId);
        }

        /**
         * Sets the region for the STS client.
         * @param region The region to set.
         * @return This StsBuilder instance.
         */
        public StsBuilder withRegion(String region) {
            this.region = region;
            this.stsBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets the endpoint to override for the STS client.
         * @param endpoint The endpoint to set.
         * @return This StsBuilder instance.
         */
        public StsBuilder withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            this.stsBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Builds and returns an StsClient instance.
         * @return A new StsClient instance.
         */
        public StsClient build() {
            this.sts = this.stsBuilder.build();
            return new StsClient(this.sts);
        }
    }
}