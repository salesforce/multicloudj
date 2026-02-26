package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ProviderSupplierTest {

    private MockedStatic<ServiceLoader> serviceLoaderStatic;
    private AbstractRegistry mockRegistry;

    @BeforeEach
    void setup() {
        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractRegistry.class)).thenReturn(serviceLoader);
        mockRegistry = mock(AbstractRegistry.class);

        when(mockRegistry.getProviderId()).thenReturn("test");
        when(serviceLoader.iterator()).thenReturn(List.of(mockRegistry).iterator());
    }

    @AfterEach
    void tearDown() {
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }
    }

    @Test
    void testNoProviderFound() {
        assertThrows(InvalidArgumentException.class,
                () -> ProviderSupplier.findProviderBuilder("www"));
    }

    @Test
    void testProviderFound() {
        AbstractRegistry.Builder mockBuilder = mock(AbstractRegistry.Builder.class);
        when(mockRegistry.builder()).thenReturn(mockBuilder);

        AbstractRegistry.Builder<?, ?> builder = ProviderSupplier.findProviderBuilder("test");
        assertNotNull(builder);
    }

    @Test
    void testReflectionFailed() {
        when(mockRegistry.builder()).thenThrow(new RuntimeException("reflection error"));

        assertThrows(RuntimeException.class,
                () -> ProviderSupplier.findProviderBuilder("test"));
    }
}
