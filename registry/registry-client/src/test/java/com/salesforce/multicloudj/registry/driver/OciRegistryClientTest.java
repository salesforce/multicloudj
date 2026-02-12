package com.salesforce.multicloudj.registry.driver;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for OciRegistryClient.
 * Tests authentication header generation for Basic, Bearer, and Anonymous auth schemes.
 */
public class OciRegistryClientTest {

    private static final String REGISTRY_ENDPOINT = "https://test-registry.example.com";
    private static final String REPOSITORY = "test-repo/test-image";

    @Mock
    private AuthProvider mockAuthProvider;

    private AutoCloseable mocks;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    void testGetHttpAuthHeader_BasicAuth() throws IOException {
        // Setup mock for Basic auth challenge
        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");

        when(mockAuthProvider.getAuthUsername()).thenReturn("testuser");
        when(mockAuthProvider.getAuthToken()).thenReturn("testtoken");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();
            mockedAuthChallenge.when(AuthChallenge::anonymous).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            String authHeader = client.getHttpAuthHeader(REPOSITORY);

            assertNotNull(authHeader);
            assertTrue(authHeader.startsWith("Basic "));

            // Verify the encoded credentials
            String expectedCredentials = "testuser:testtoken";
            String expectedEncoded = Base64.getEncoder().encodeToString(
                    expectedCredentials.getBytes(StandardCharsets.UTF_8));
            assertEquals("Basic " + expectedEncoded, authHeader);

            client.close();
        }
    }

    @Test
    void testGetHttpAuthHeader_BearerAuth() throws IOException {
        // Setup mock for Bearer auth challenge
        AuthChallenge bearerChallenge = AuthChallenge.parse(
                "Bearer realm=\"https://auth.example.com/token\",service=\"registry.example.com\"");

        when(mockAuthProvider.getAuthToken()).thenReturn("identity-token");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class);
             MockedConstruction<BearerTokenExchange> mockedExchange = mockConstruction(
                     BearerTokenExchange.class,
                     (mock, context) -> when(mock.getBearerToken(any(), anyString(), anyString(), any()))
                             .thenReturn("bearer-token-123"))) {

            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(bearerChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            String authHeader = client.getHttpAuthHeader(REPOSITORY);

            assertNotNull(authHeader);
            assertEquals("Bearer bearer-token-123", authHeader);

            client.close();
        }
    }

    @Test
    void testGetHttpAuthHeader_AnonymousAuth() throws IOException {
        // Setup mock for anonymous auth (no auth required)
        AuthChallenge anonymousChallenge = AuthChallenge.anonymous();

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(anonymousChallenge);
            mockedAuthChallenge.when(AuthChallenge::anonymous).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            String authHeader = client.getHttpAuthHeader(REPOSITORY);

            assertNull(authHeader);

            client.close();
        }
    }

    @Test
    void testGetHttpAuthHeader_UnsupportedScheme() throws IOException {
        AuthChallenge mockChallenge = mock(AuthChallenge.class);
        when(mockChallenge.getScheme()).thenReturn("unsupported");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(mockChallenge);

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            assertThrows(UnsupportedOperationException.class, () -> client.getHttpAuthHeader(REPOSITORY));

            client.close();
        }
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testGetHttpAuthHeader_BlankOrNullScheme(String scheme) throws IOException {
        AuthChallenge mockChallenge = mock(AuthChallenge.class);
        when(mockChallenge.getScheme()).thenReturn(scheme);

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(mockChallenge);

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            assertThrows(IllegalArgumentException.class, () -> client.getHttpAuthHeader(REPOSITORY));

            client.close();
        }
    }

    @Test
    void testGetHttpAuthHeader_CachesChallenge() throws IOException {
        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");

        when(mockAuthProvider.getAuthUsername()).thenReturn("testuser");
        when(mockAuthProvider.getAuthToken()).thenReturn("testtoken");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

            // Call twice
            client.getHttpAuthHeader(REPOSITORY);
            client.getHttpAuthHeader(REPOSITORY);

            // Verify discover was called only once (challenge is cached)
            mockedAuthChallenge.verify(
                    () -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()),
                    times(1));

            client.close();
        }
    }

    @Test
    void testFetchManifest_NotImplemented() throws IOException {
        OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

        assertThrows(UnsupportedOperationException.class,
                () -> client.fetchManifest(REPOSITORY, "latest"));

        client.close();
    }

    @Test
    void testDownloadBlob_NotImplemented() throws IOException {
        OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider);

        assertThrows(UnsupportedOperationException.class,
                () -> client.downloadBlob(REPOSITORY, "sha256:abc123"));

        client.close();
    }
}
