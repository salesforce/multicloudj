package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;

import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

public class ProviderSupplier {

    private ProviderSupplier() {}

    public static AsyncBlobStoreProvider.Builder findAsyncBuilder(String providerId) {
        ServiceLoader<AsyncBlobStoreProvider> loader = ServiceLoader.load(AsyncBlobStoreProvider.class);
        // stream through the providers to find our provider id
        // if we can't find one, just throw an IllegalArgumentException:
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(p -> p.getProviderId().equals(providerId))
                .findFirst()
                .map(AsyncBlobStoreProvider::builder)
                .orElseThrow(() -> new IllegalArgumentException("No provider found for " + providerId));
    }
}
