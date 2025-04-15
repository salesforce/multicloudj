package com.salesforce.multicloudj.blob.async.client;

import com.salesforce.multicloudj.blob.async.driver.AsyncBlobStoreProvider;
import com.salesforce.multicloudj.blob.async.driver.TestAsyncBlobStore;
import com.salesforce.multicloudj.blob.async.driver.TestAsyncBlobStoreProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
public class ProviderSupplierTest {

    @Test
    public void testProvider() {
        // wrap in try to auto-close the static mock, any exception will be thrown:
        try (MockedStatic<ServiceLoader> serviceLoaderStatic = mockStatic(ServiceLoader.class)) {
            ServiceLoader serviceLoader = mock(ServiceLoader.class);
            serviceLoaderStatic.when(() -> ServiceLoader.load(AsyncBlobStoreProvider.class)).thenReturn(serviceLoader);
            serviceLoaderStatic.when(() -> ServiceLoader.loadInstalled(AsyncBlobStoreProvider.class)).thenReturn(serviceLoader);
            AsyncBlobStoreProvider provider = new TestAsyncBlobStoreProvider();

            List<AsyncBlobStoreProvider> providers = List.of(provider);
            Spliterator<AsyncBlobStoreProvider> providerSpliterator = providers.spliterator();
            when(serviceLoader.iterator()).thenReturn(providers.iterator());
            when(serviceLoader.spliterator()).thenReturn(providerSpliterator);

            AsyncBlobStoreProvider.Builder builder = ProviderSupplier.findAsyncBuilder(TestAsyncBlobStore.PROVIDER_ID);
            Assertions.assertInstanceOf(TestAsyncBlobStore.Builder.class, builder);
            Assertions.assertNotNull(builder);
        }
    }
}
