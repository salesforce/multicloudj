package com.salesforce.multicloudj.blob.client;

import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ProviderSupplierTest {

    private MockedStatic<ServiceLoader> serviceLoaderStatic;
    private AbstractBlobStore mockBlobStore;
    private AbstractBlobClient mockBlobClient;

    @BeforeEach
    void setup() {
        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractBlobStore.class)).thenReturn(serviceLoader);
        mockBlobStore = mock(AbstractBlobStore.class);
        mockBlobClient = mock(AbstractBlobClient.class);

        when(mockBlobStore.getProviderId()).thenReturn("test");
        Iterator<AbstractBlobStore> providerIterator = List.of(mockBlobStore).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);


        ServiceLoader serviceLoader1 = mock(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractBlobClient.class)).thenReturn(serviceLoader1);
        when(mockBlobClient.getProviderId()).thenReturn("testClient");
        Iterator<AbstractBlobClient> providerClientIterator = List.of(mockBlobClient).iterator();
        when(serviceLoader1.iterator()).thenReturn(providerClientIterator);
    }

    @AfterEach
    void testdown() {
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }
    }

    @Test
    void testNoProviderFound() {
        assertThrows(IllegalArgumentException.class, () -> {
            ProviderSupplier.findProviderBuilder("www");
        });
    }

    @Test
    void testProviderFound() {
        AbstractBlobStore.Builder mockBuilder = mock(AbstractBlobStore.Builder.class);
        when(mockBlobStore.builder()).thenReturn(mockBuilder);

        AbstractBlobStore.Builder builder = ProviderSupplier.findProviderBuilder("test");
        assertNotNull(builder);
    }

    @Test
    void testReflectionFailed() {
        when(mockBlobStore.builder()).thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class, () -> {
            ProviderSupplier.findProviderBuilder("test");
        });
    }

    @Test
    void testBlobClientProviderFound() {
        AbstractBlobClient.Builder mockBuilder = mock(AbstractBlobClient.Builder.class);
        when(mockBlobClient.builder()).thenReturn(mockBuilder);

        AbstractBlobClient.Builder builder = ProviderSupplier.findBlobClientProviderBuilder("testClient");
        assertNotNull(builder);
    }
}
