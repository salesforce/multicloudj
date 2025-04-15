package com.salesforce.multicloudj.sts.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.net.http.HttpRequest;
import java.util.ServiceLoader;

/**
 * StsUtilities class provides portable offline Utility functions which utilize the Security Token Service (STS)
 * in a substrate agnostic way.
 */
public class StsUtilities {
    protected AbstractStsUtilities<?> stsUtility;

    /**
     * Constructor for StsUtilities with StsUtilityBuilder.
     *
     * @param builder The StsUtilityBuilder used to construct this utility.
     */
    protected StsUtilities(StsUtilityBuilder builder) {
        this.stsUtility = builder.stsUtility;
    }

    /**
     * Creates a new StsUtilityBuilder for the specified provider.
     *
     * @param providerId The ID of the provider/substrate such as aws.
     * @return A new StsUtilityBuilder instance.
     */
    public static StsUtilityBuilder builder(String providerId) {
        return new StsUtilityBuilder(providerId);
    }

    /**
     * Returns an Iterable of all available AbstractStsUtilities implementations.
     *
     * @return An Iterable of AbstractStsUtilities instances.
     */
    private static Iterable<AbstractStsUtilities<?>> all() {
        ServiceLoader<AbstractStsUtilities> services = ServiceLoader.load(AbstractStsUtilities.class);
        ImmutableSet.Builder<AbstractStsUtilities<?>> builder = ImmutableSet.builder();
        for (AbstractStsUtilities<?> service : services) {
            builder.add(service);
        }
        return builder.build();
    }

    /**
     * Finds the builder for the specified provider.
     *
     * @param providerId The ID of the provider.
     * @return The AbstractStsUtilities.Builder for the specified provider.
     * @throws IllegalArgumentException if no provider is found for the given ID.
     */
    private static AbstractStsUtilities.Builder<?> findProviderBuilder(String providerId) {
        for (AbstractStsUtilities<?> provider : all()) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No cloud storage provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     *
     * @param provider The AbstractStsUtilities provider.
     * @return The AbstractStsUtilities.Builder for the provider.
     * @throws RuntimeException if the builder creation fails.
     */
    private static AbstractStsUtilities.Builder<?> createBuilderInstance(AbstractStsUtilities<?> provider) {
        try {
            return (AbstractStsUtilities.Builder<?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }

    /**
     * Assumes a role and returns the temporary credentialsOverrider for that role.
     *
     * @param request The AssumedRoleRequest.
     * @return The StsCredentials for the assumed role.
     */
    public SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
        try {
            return this.stsUtility.cloudNativeAuthSignedRequest(request);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.stsUtility.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Builder class for StsUtilities.
     */
    public static class StsUtilityBuilder {
        protected AbstractStsUtilities<?> stsUtility;
        protected AbstractStsUtilities.Builder<?> builder;

        /**
         * Constructor for StsUtilityBuilder.
         *
         * @param providerId The ID of the provider such as aws.
         */
        public StsUtilityBuilder(String providerId) {
            this.builder = findProviderBuilder(providerId);
        }

        /**
         * Sets the region for the STS Utilities.
         *
         * @param region The region to set.
         * @return This StsUtilityBuilder instance.
         */
        public StsUtilityBuilder withRegion(String region) {
            this.builder.withRegion(region);
            return this;
        }

        /**
         * Sets the credentialsOverrider for the STS Utilities.
         *
         * @param credentialsOverrider The credentialsOverrider overrider to supply the credentialsOverrider
         * @return This StsUtilityBuilder instance.
         */
        public StsUtilityBuilder withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.builder.withCredentialsOverrider(credentialsOverrider);
            return this;
        }

        /**
         * Builds and returns an StsUtilities instance.
         *
         * @return A new StsUtilities instance.
         */
        public StsUtilities build() {
            this.stsUtility = this.builder.build();
            return new StsUtilities(this);
        }
    }
}