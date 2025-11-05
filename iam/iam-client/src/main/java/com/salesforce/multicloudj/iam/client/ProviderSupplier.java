package com.salesforce.multicloudj.iam.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.iam.driver.AbstractIam;

import java.util.ServiceLoader;

/**
 * This class will attempt to find an IAM provider in the classpath by providerId
 */
public class ProviderSupplier {

    private ProviderSupplier() {
    }

    /**
     * Finds the provider builder for the specified provider ID.
     *
     * @param providerId Id of the provider to be found
     * @return Builder object for the provider
     * @throws IllegalArgumentException if no provider is found for the given ID
     */
    static AbstractIam.Builder<?, ?> findProviderBuilder(String providerId) {
        ServiceLoader<AbstractIam> services = ServiceLoader.load(AbstractIam.class);
        ImmutableSet.Builder<AbstractIam<?>> builder = ImmutableSet.builder();
        for (AbstractIam<?> service : services) {
            builder.add(service);
        }
        Iterable<AbstractIam<?>> all = builder.build();

        for (AbstractIam<?> provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No IAM provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     *
     * @param provider The AbstractIam provider.
     * @return The AbstractIam.Builder for the provider.
     * @throws RuntimeException if the builder creation fails.
     */
    private static AbstractIam.Builder<?, ?> createBuilderInstance(AbstractIam<?> provider) {
        try {
            return (AbstractIam.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }
}