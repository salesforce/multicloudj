package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StsClientTest {
    @Test
    public void testStsClient() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);


        // Mock the ServiceLoader to return mockProvider
        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
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

    @Test
    public void testGetCallerIdentityWithRequest() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockProvider);

        CallerIdentity mockIdentity = new CallerIdentity("userId", "arn", "accountId");
        when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class))).thenReturn(mockIdentity);

        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            StsClient client = StsClient.builder("mockProviderId").build();
            GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().aud("custom-audience").build();
            CallerIdentity identity = client.getCallerIdentity(request);

            assertNotNull(identity);
            assertEquals("userId", identity.getUserId());
            assertEquals("arn", identity.getCloudResourceName());
            assertEquals("accountId", identity.getAccountId());
        }
    }

    @Test
    public void testGetAssumeRoleWithWebIdentityCredentials() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockProvider);

        StsCredentials mockCredentials = new StsCredentials("accessKey", "secretKey", "sessionToken");
        when(mockProvider.assumeRoleWithWebIdentity(any(AssumeRoleWebIdentityRequest.class))).thenReturn(mockCredentials);

        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            StsClient client = StsClient.builder("mockProviderId").build();
            AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                    .role("test-role")
                    .webIdentityToken("test-token")
                    .sessionName("test-session")
                    .build();
            StsCredentials credentials = client.getAssumeRoleWithWebIdentityCredentials(request);

            assertNotNull(credentials);
            assertEquals("accessKey", credentials.getAccessKeyId());
            assertEquals("secretKey", credentials.getAccessKeySecret());
            assertEquals("sessionToken", credentials.getSecurityToken());
        }
    }

    @Test
    public void testGetCallerIdentityNoArgsThrowsException() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockProvider);

        when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
                .thenThrow(new RuntimeException("Test exception"));
        when(mockProvider.getException(any(Throwable.class))).thenAnswer(invocation -> UnknownException.class);

        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            StsClient client = StsClient.builder("mockProviderId").build();
            assertThrows(SubstrateSdkException.class, () -> client.getCallerIdentity());
        }
    }

    @Test
    public void testGetCallerIdentityWithRequestThrowsException() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockProvider);

        when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
                .thenThrow(new RuntimeException("Test exception"));
        when(mockProvider.getException(any(Throwable.class))).thenAnswer(invocation -> UnknownException.class);

        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            StsClient client = StsClient.builder("mockProviderId").build();
            GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().aud("test-audience").build();
            assertThrows(SubstrateSdkException.class, () -> client.getCallerIdentity(request));
        }
    }

    @Test
    public void testGetAssumeRoleWithWebIdentityCredentialsThrowsException() {
        AbstractSts mockProvider = mock(AbstractSts.class);
        AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
        when(mockProvider.getProviderId()).thenReturn("mockProviderId");
        when(mockProvider.builder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockProvider);

        when(mockProvider.assumeRoleWithWebIdentity(any(AssumeRoleWebIdentityRequest.class)))
                .thenThrow(new RuntimeException("Test exception"));
        when(mockProvider.getException(any(Throwable.class))).thenAnswer(invocation -> UnknownException.class);

        ServiceLoader serviceLoader = mock(ServiceLoader.class);
        Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractSts.class)).thenReturn(serviceLoader);

            StsClient client = StsClient.builder("mockProviderId").build();
            AssumeRoleWebIdentityRequest request = AssumeRoleWebIdentityRequest.builder()
                    .role("test-role")
                    .webIdentityToken("test-token")
                    .sessionName("test-session")
                    .build();
            assertThrows(SubstrateSdkException.class, () -> client.getAssumeRoleWithWebIdentityCredentials(request));
        }
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