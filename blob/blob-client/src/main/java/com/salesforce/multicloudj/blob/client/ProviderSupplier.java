package com.salesforce.multicloudj.blob.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;

import java.util.ServiceLoader;

/**
 * This class will attempt to find a BlobStore provider in the classpath by providerId
 */
public class ProviderSupplier {

    private ProviderSupplier() {
    }

    /**
     * Finds the provider
     *
     * @param providerId Id of the provider to be found
     * @return Builder object for the provider
     */
    static AbstractBlobStore.Builder findProviderBuilder(String providerId) {
        ServiceLoader<AbstractBlobStore> services = ServiceLoader.load(AbstractBlobStore.class);
        Iterable<AbstractBlobStore> all = ImmutableSet.<AbstractBlobStore>builder().addAll(services).build();

        for (AbstractBlobStore provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No BlobStore provider found for providerId: " + providerId);
    }

    /**
     * Finds the BlobClient provider
     * @param providerId
     * @return Builder object for the blobClient provider
     */
    static AbstractBlobClient.Builder findBlobClientProviderBuilder(String providerId) {
        ServiceLoader<AbstractBlobClient> services = ServiceLoader.load(AbstractBlobClient.class);
        Iterable<AbstractBlobClient> all = ImmutableSet.<AbstractBlobClient>builder().addAll(services).build();

        for (AbstractBlobClient provider : all) {
            if (provider.getProviderId().equals(providerId)) {
                return createBlobClientBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No BlobClient provider found for providerId: " + providerId);
    }

    private static AbstractBlobStore.Builder<?, ?> createBuilderInstance(AbstractBlobStore provider) {
        try {
            return (AbstractBlobStore.Builder<?, ?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
    }

    private static AbstractBlobClient.Builder<?> createBlobClientBuilderInstance(AbstractBlobClient<?> provider) {
        try {
            return (AbstractBlobClient.Builder<?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create blob client builder for provider: " + provider.getClass().getName(), e);
        }
    }
}
