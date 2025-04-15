package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StsUtilitiesTest {

    @Test
    public void testStsUtilities() {
        AbstractStsUtilities<?> mockProvider = mock(AbstractStsUtilities.class);
        AbstractStsUtilities.Builder<?> mockBuilder = mock(AbstractStsUtilities.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);

        // Mock the ServiceLoader to return mockProvider
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractStsUtilities<?>> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, false);
    }

    @Test
    public void testStsUtilitiesWithTestProvider() {
        TestConcreteAbstractStsUtilities provider = new TestConcreteAbstractStsUtilities();

        // Mock the ServiceLoader to return mockProvider
        ServiceLoader<TestConcreteAbstractStsUtilities> serviceLoader = mock(ServiceLoader.class);
        Iterator<TestConcreteAbstractStsUtilities> providerIterator = List.of(provider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        verifyServiceLoader(serviceLoader, true);
    }

    private void verifyServiceLoader(ServiceLoader<?> serviceLoader, boolean useTestProvider) {
        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractStsUtilities.class)).thenReturn(serviceLoader);

            CredentialsOverrider mockCreds = mock(CredentialsOverrider.class);
            // Execute the test logic that relies on the mocked ServiceLoader
            StsUtilities.StsUtilityBuilder builder = StsUtilities.builder("mockProviderId");
            builder.withRegion("test-region");
            builder.withCredentialsOverrider(mockCreds);
            StsUtilities client = builder.build();

            // Assertions to verify the expected behavior
            assertNotNull(client);
            if (useTestProvider) {
                assertEquals("mockProviderId", client.stsUtility.getProviderId());
            }
        }
    }

}