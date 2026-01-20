package com.salesforce.multicloudj.registry.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;

import java.util.ServiceLoader;

/**
 * This class will attempt to find a Registry provider in the classpath by providerId
 */
public class ProviderSupplier {

    private ProviderSupplier() {
    }

    /**
     * Finds the Registry provider (resource-level)
     *
     * @param providerId Id of the provider to be found
     * @return Builder object for the provider
     */
    static AbstractRegistry.Builder<?, ?> findRegistryProviderBuilder(String providerId) {
        ServiceLoader<AbstractRegistry> services = ServiceLoader.load(AbstractRegistry.class);
        Iterable<AbstractRegistry> all = ImmutableSet.<AbstractRegistry>builder().addAll(services).build();

        for (AbstractRegistry provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No Registry provider found for providerId: " + providerId);
    }

    private static AbstractRegistry.Builder<?, ?> createBuilderInstance(AbstractRegistry provider) {
        try {
            return (AbstractRegistry.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }
}
