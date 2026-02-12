package com.salesforce.multicloudj.registry.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;

import java.util.Objects;
import java.util.ServiceLoader;

/** Finds a registry provider by provider ID (ServiceLoader). */
public class ProviderSupplier {

    private ProviderSupplier() {
    }

    /**
     * @param providerId provider to find (e.g. aws, gcp, ali)
     * @return builder for that provider
     * @throws IllegalArgumentException if no provider is found
     */
    static AbstractRegistry.Builder<?, ?> findProviderBuilder(String providerId) {
        ServiceLoader<AbstractRegistry> services = ServiceLoader.load(AbstractRegistry.class);
        Iterable<AbstractRegistry> all = ImmutableSet.<AbstractRegistry>builder().addAll(services).build();

        for (AbstractRegistry provider : all) {
            if (Objects.equals(provider.getProviderId(), providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No Registry provider found for providerId: " + providerId);
    }

    /** Creates a builder instance for the provider. */
    private static AbstractRegistry.Builder<?, ?> createBuilderInstance(AbstractRegistry provider) {
        try {
            return (AbstractRegistry.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }
}
