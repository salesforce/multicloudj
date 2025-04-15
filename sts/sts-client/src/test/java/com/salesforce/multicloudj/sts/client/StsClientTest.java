package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.sts.driver.AbstractSts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StsClientTest {
    @Test
    public void testStsClient() {
        AbstractSts<?> mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder<?> mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);


        // Mock the ServiceLoader to return mockProvider
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts<?>> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, false);
    }

    @Test
    public void testStsClientWithTestProvider() {
        TestConcreteAbstractSts provider = new TestConcreteAbstractSts();


        // Mock the ServiceLoader to return mockProvider
        ServiceLoader<TestConcreteAbstractSts> serviceLoader = mock(ServiceLoader.class);
        Iterator<TestConcreteAbstractSts> providerIterator = List.of(provider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, true);
    }

    private void verifyServiceLoader(ServiceLoader<?> serviceLoader, boolean useTestProvider) {
        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            // Execute the test logic that relies on the mocked ServiceLoader
            StsClient.StsBuilder builder = StsClient.builder("mockProviderId");
            builder.withRegion("test-region");
            builder.withEndpoint(URI.create("https://myendpoint.com"));

            StsClient client = builder.build();

            // Assertions to verify the expected behavior
            assertNotNull(client);
            if (useTestProvider) {
                assertEquals("mockProviderId", client.sts.getProviderId());
            }
        }
    }
}