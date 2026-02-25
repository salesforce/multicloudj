package com.salesforce.multicloudj.registry.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class GcpRegistryTest {

    private static final String TEST_REGISTRY_ENDPOINT = "https://us-central1-docker.pkg.dev";

    @FunctionalInterface
    interface RegistryTestAction {
        void execute(GcpRegistry registry) throws Exception;
    }

    private void withMockedRegistry(RegistryTestAction action) throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();
            try {
                action.execute(registry);
            } finally {
                registry.close();
            }
        }
    }

    @Test
    void testBuilderAndBasicProperties() throws Exception {
        withMockedRegistry(registry -> {
            assertNotNull(registry);
            assertEquals("gcp", registry.getProviderId());
            assertEquals("oauth2accesstoken", registry.getAuthUsername());
            assertNotNull(registry.getOciClient());
            assertNotNull(registry.builder());
        });
    }

    @Test
    void testGetAuthToken_Success() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            AccessToken accessToken = new AccessToken("test-token-value", new Date(System.currentTimeMillis() + 3600000));

            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            assertEquals("test-token-value", registry.getAuthToken());
            registry.close();
        }
    }

    @Test
    void testGetAuthToken_NullAccessToken_ThrowsUnknownException() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);

            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            when(scopedCredentials.getAccessToken()).thenReturn(null);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals("Failed to obtain GCP access token: access token is null", 
                    exception.getMessage());
            registry.close();
        }
    }

    @Test
    void testGetAuthToken_NullTokenValue_ThrowsUnknownException() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            AccessToken accessToken = mock(AccessToken.class);

            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
            when(accessToken.getTokenValue()).thenReturn(null);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals("Failed to obtain GCP access token: token value is null", 
                    exception.getMessage());
            registry.close();
        }
    }

    @Test
    void testGetException_WithSubstrateSdkException() throws Exception {
        withMockedRegistry(registry -> {
            SubstrateSdkException testException = new SubstrateSdkException("Test");
            assertEquals(SubstrateSdkException.class, registry.getException(testException));
        });
    }

    @Test
    void testGetException_WithApiException() throws Exception {
        withMockedRegistry(registry -> {
            StatusCode mockStatusCode = mock(StatusCode.class);
            when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
            ApiException apiException = mock(ApiException.class);
            when(apiException.getStatusCode()).thenReturn(mockStatusCode);

            assertEquals(ResourceNotFoundException.class, registry.getException(apiException));
        });
    }

    @Test
    void testGetException_WithIllegalArgumentException() throws Exception {
        withMockedRegistry(registry -> {
            assertEquals(InvalidArgumentException.class, 
                    registry.getException(new IllegalArgumentException("Invalid")));
        });
    }

    @Test
    void testGetException_WithUnknownException() throws Exception {
        withMockedRegistry(registry -> {
            assertEquals(UnknownException.class,
                    registry.getException(new RuntimeException("Test")));
        });
    }

    @Test
    void testGetAuthToken_TokenCaching() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            AccessToken accessToken = new AccessToken("cached-token", new Date(System.currentTimeMillis() + 3600000));

            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            registry.getAuthToken();
            registry.getAuthToken();

            // getApplicationDefault should only be called once — credentials are cached
            mockedStatic.verify(GoogleCredentials::getApplicationDefault, org.mockito.Mockito.times(1));

            registry.close();
        }
    }

    @Test
    void testBuilder_BlankRegistryEndpoint_ThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> new GcpRegistry.Builder().build());
        assertThrows(InvalidArgumentException.class, () -> new GcpRegistry.Builder().withRegistryEndpoint("").build());
        assertThrows(InvalidArgumentException.class, () -> new GcpRegistry.Builder().withRegistryEndpoint("   ").build());
    }

    @Test
    void testBuilder_WithPlatform() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            com.salesforce.multicloudj.registry.model.Platform customPlatform = 
                    com.salesforce.multicloudj.registry.model.Platform.builder()
                            .operatingSystem("linux")
                            .architecture("arm64")
                            .build();

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .withPlatform(customPlatform)
                    .build();

            assertNotNull(registry.getTargetPlatform());
            assertEquals("linux", registry.getTargetPlatform().getOperatingSystem());
            assertEquals("arm64", registry.getTargetPlatform().getArchitecture());

            registry.close();
        }
    }

    @Test
    void testBuilder_WithoutPlatform_UsesDefault() throws Exception {
        withMockedRegistry(registry -> {
            assertNotNull(registry.getTargetPlatform());
            assertEquals("linux", registry.getTargetPlatform().getOperatingSystem());
            assertEquals("amd64", registry.getTargetPlatform().getArchitecture());
        });
    }

    @Test
    void testCredentialsOverrider_ReturnsNull_ThrowsException() throws Exception {
        try (MockedStatic<com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider> 
                mockedProvider = mockStatic(
                        com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider.class)) {

            // Mock GcpCredentialsProvider to return null
            mockedProvider.when(() ->
                    com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider.getCredentials(
                            org.mockito.ArgumentMatchers.any()))
                    .thenReturn(null);

            com.salesforce.multicloudj.sts.model.CredentialsOverrider overrider =
                    mock(com.salesforce.multicloudj.sts.model.CredentialsOverrider.class);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .withCredentialsOverrider(overrider)
                    .build();

            // Attempting to get auth token should fail when overrider produces no credentials
            UnknownException exception = assertThrows(UnknownException.class, registry::getAuthToken);
            assertEquals("Failed to obtain credentials from CredentialsOverrider", 
                    exception.getMessage());

            registry.close();
        }
    }

    @Test
    void testCreateGoogleCredentials_WithCredentialsOverrider_Success() throws Exception {
        try (MockedStatic<com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider> mp =
                     mockStatic(com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider.class)) {
            GoogleCredentials creds = mock(GoogleCredentials.class);
            GoogleCredentials scoped = mock(GoogleCredentials.class);
            when(creds.createScoped(anyList())).thenReturn(scoped);
            when(scoped.getAccessToken()).thenReturn(
                    new AccessToken("overrider-token", new Date(System.currentTimeMillis() + 3600000)));
            mp.when(() -> com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider.getCredentials(any()))
                    .thenReturn(creds);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .withCredentialsOverrider(mock(com.salesforce.multicloudj.sts.model.CredentialsOverrider.class))
                    .build();
            assertEquals("overrider-token", registry.getAuthToken());
        }
    }

    @Test
    void testCreateGoogleCredentials_ApplicationDefaultNull_ThrowsUnknownException() throws Exception {
        try (MockedStatic<GoogleCredentials> m = mockStatic(GoogleCredentials.class)) {
            m.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
            GcpRegistry registry = new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
            assertTrue(assertThrows(UnknownException.class, registry::getAuthToken)
                    .getMessage().contains("application default credentials not available"));
        }
    }

    @Test
    void testCreateGoogleCredentials_IOExceptionWrappedInUnknownException() throws Exception {
        try (MockedStatic<GoogleCredentials> m = mockStatic(GoogleCredentials.class)) {
            m.when(GoogleCredentials::getApplicationDefault).thenThrow(new IOException("fail"));
            GcpRegistry registry = new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
            assertTrue(assertThrows(UnknownException.class, registry::getAuthToken)
                    .getMessage().contains("Failed to load GCP credentials"));
        }
    }

    @Test
    void testGetOrCreateCredentials_RefreshIOExceptionWrappedInUnknownException() throws Exception {
        try (MockedStatic<GoogleCredentials> m = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials creds = mock(GoogleCredentials.class);
            GoogleCredentials scoped = mock(GoogleCredentials.class);
            when(creds.createScoped(anyList())).thenReturn(scoped);
            doThrow(new IOException("refresh fail")).when(scoped).refreshIfExpired();
            m.when(GoogleCredentials::getApplicationDefault).thenReturn(creds);

            GcpRegistry registry = new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
            assertTrue(assertThrows(UnknownException.class, registry::getAuthToken)
                    .getMessage().contains("Failed to refresh GCP credentials"));
        }
    }
}
