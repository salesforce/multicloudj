package com.salesforce.multicloudj.registry.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
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
    void testGetAuthToken_NullAccessToken_ThrowsIOException() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);

            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            when(scopedCredentials.getAccessToken()).thenReturn(null);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            IOException exception = assertThrows(IOException.class, registry::getAuthToken);
            assertEquals("Failed to obtain GCP access token: access token is null", 
                    exception.getMessage());
            registry.close();
        }
    }

    @Test
    void testGetAuthToken_NullTokenValue_ThrowsIOException() throws Exception {
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

            IOException exception = assertThrows(IOException.class, registry::getAuthToken);
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

            // Call getAuthToken twice
            String token1 = registry.getAuthToken();
            String token2 = registry.getAuthToken();

            // Verify same token returned (credentials cached)
            assertEquals(token1, token2);
            assertEquals("cached-token", token1);

            registry.close();
        }
    }

    @Test
    void testBuilder_MissingRegistryEndpoint_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new GcpRegistry.Builder().build();
        });
        assertEquals("Registry endpoint is required for GCP Artifact Registry", exception.getMessage());
    }

    @Test
    void testBuilder_EmptyRegistryEndpoint_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new GcpRegistry.Builder()
                    .withRegistryEndpoint("")
                    .build();
        });
        assertEquals("Registry endpoint is required for GCP Artifact Registry", exception.getMessage());
    }

    @Test
    void testBuilder_WhitespaceRegistryEndpoint_ThrowsException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new GcpRegistry.Builder()
                    .withRegistryEndpoint("   ")
                    .build();
        });
        assertEquals("Registry endpoint is required for GCP Artifact Registry", exception.getMessage());
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
    void testClose_MultipleCloses_NoError() throws Exception {
        try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
            GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
            GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
            when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
            mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

            GcpRegistry registry = new GcpRegistry.Builder()
                    .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                    .build();

            // Close multiple times should not throw exception
            registry.close();
            registry.close();
        }
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
            IOException exception = assertThrows(IOException.class, registry::getAuthToken);
            assertEquals("Failed to obtain credentials from CredentialsOverrider", 
                    exception.getMessage());

            registry.close();
        }
    }
}
