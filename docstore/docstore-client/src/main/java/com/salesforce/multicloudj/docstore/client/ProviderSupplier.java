package com.salesforce.multicloudj.docstore.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;

import java.util.ServiceLoader;

public class ProviderSupplier {
    private ProviderSupplier() {}

    /**
     * Finds the provider
     * @param providerId Id of the provider to be found
     * @return Builder object for the provider
     */
    static AbstractDocStore.Builder findProviderBuilder(String providerId) {
        ServiceLoader<AbstractDocStore> services = ServiceLoader.load(AbstractDocStore.class);
        Iterable<AbstractDocStore> all = ImmutableSet.<AbstractDocStore>builder().addAll(services).build();

        for (AbstractDocStore provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No DocStore provider found for providerId: " + providerId);
    }

    private static AbstractDocStore.Builder<?, ?> createBuilderInstance(AbstractDocStore provider) {
        try {
            return (AbstractDocStore.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }
}
