package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.iam.driver.AbstractIam;
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
public class IamClientTest {
    @Test
    public void testIamClient() {
        AbstractIam<?> mockProvider = mock(AbstractIam.class);
        AbstractIam.Builder<?> mockBuilder = mock(AbstractIam.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);

        // Mock the ServiceLoader to return mockProvider
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractIam<?>> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, false);
    }

    @Test
    public void testIamClientWithTestProvider() {
        TestIam provider = new TestIam();

        // Mock the ServiceLoader to return mockProvider
        ServiceLoader<TestIam> serviceLoader = mock(ServiceLoader.class);
        Iterator<TestIam> providerIterator = List.of(provider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, true);
    }

    private void verifyServiceLoader(ServiceLoader<?> serviceLoader, boolean useTestProvider) {
        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(serviceLoader);

            // Execute the test logic that relies on the mocked ServiceLoader
            IamClient.IamClientBuilder builder = IamClient.builder("mockProviderId");
            builder.withRegion("us-west-2");
            builder.withEndpoint(URI.create("https://test-endpoint.com"));

            IamClient client = builder.build();

            // Assertions to verify the expected behavior
            assertNotNull(client);
            if (useTestProvider) {
                assertEquals("mockProviderId", client.iam.getProviderId());
            }
        }
    }
}

