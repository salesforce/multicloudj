package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.registry.model.Manifest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    // ========== fetchManifest Tests ==========

    @Test
    void testFetchManifest_SingleImageManifest_WithDockerDigestHeader() throws IOException {
        String manifestJson = "{"
                + "\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\","
                + "\"config\":{\"digest\":\"sha256:config123\",\"size\":1234},"
                + "\"layers\":["
                + "{\"digest\":\"sha256:layer1\",\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"size\":5678},"
                + "{\"digest\":\"sha256:layer2\",\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"size\":9012}"
                + "]"
                + "}";

        testFetchManifestWithResponse(manifestJson, "sha256:manifestdigest", HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertEquals("sha256:manifestdigest", manifest.getDigest());
            assertEquals("sha256:config123", manifest.getConfigDigest());
            assertNotNull(manifest.getLayerInfos());
            assertEquals(2, manifest.getLayerInfos().size());
            assertEquals("sha256:layer1", manifest.getLayerInfos().get(0).getDigest());
            assertEquals("application/vnd.oci.image.layer.v1.tar+gzip",
                    manifest.getLayerInfos().get(0).getMediaType());
            assertEquals(5678L, manifest.getLayerInfos().get(0).getSize());
        });
    }

    @Test
    void testFetchManifest_SingleImageManifest_WithoutDockerDigestHeader() throws IOException {
        String manifestJson = "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"digest\":\"sha256:lyr\"}]}";

        testFetchManifestWithResponse(manifestJson, null, HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertNotNull(manifest.getDigest());
            assertTrue(manifest.getDigest().startsWith("sha256:"));
            assertEquals("sha256:cfg", manifest.getConfigDigest());
        });
    }

    @Test
    void testFetchManifest_ImageIndex_WithManifestsArray() throws IOException {
        String indexJson = "{"
                + "\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":["
                + "{\"digest\":\"sha256:amd64manifest\",\"platform\":{\"os\":\"linux\",\"architecture\":\"amd64\"}},"
                + "{\"digest\":\"sha256:arm64manifest\",\"platform\":{\"os\":\"linux\",\"architecture\":\"arm64\"}}"
                + "]"
                + "}";

        testFetchManifestWithResponse(indexJson, "sha256:indexdigest", HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertEquals("sha256:indexdigest", manifest.getDigest());
            assertNotNull(manifest.getIndexManifests());
            assertEquals(2, manifest.getIndexManifests().size());
            assertEquals("sha256:amd64manifest", manifest.getIndexManifests().get(0).getDigest());
            assertEquals("linux", manifest.getIndexManifests().get(0).getOs());
            assertEquals("amd64", manifest.getIndexManifests().get(0).getArchitecture());
        });
    }

    @Test
    void testFetchManifest_ImageIndex_WithEnhancedPlatformFields() throws IOException {
        String indexJson = "{"
                + "\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":["
                + "{\"digest\":\"sha256:manifest1\",\"platform\":{"
                + "\"os\":\"windows\",\"architecture\":\"amd64\","
                + "\"os.version\":\"10.0.19041\","
                + "\"variant\":\"v8\","
                + "\"os.features\":[\"win32k\",\"hyperv\"]"
                + "}}"
                + "]"
                + "}";

        testFetchManifestWithResponse(indexJson, "sha256:idx", HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertEquals(1, manifest.getIndexManifests().size());
            Manifest.IndexEntry entry = manifest.getIndexManifests().get(0);
            assertEquals("windows", entry.getOs());
            assertEquals("amd64", entry.getArchitecture());
        });
    }

    @Test
    void testFetchManifest_ImageIndex_NullManifestsArray() throws IOException {
        String indexJson = "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.index.v1+json\"}";

        testFetchManifestWithResponse(indexJson, "sha256:emptyindex", HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertNotNull(manifest.getIndexManifests());
            assertTrue(manifest.getIndexManifests().isEmpty());
        });
    }

    @Test
    void testFetchManifest_ImageIndex_WithAnnotations() throws IOException {
        String indexJson = "{"
                + "\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":[{\"digest\":\"sha256:m1\"}],"
                + "\"annotations\":{\"key1\":\"value1\",\"key2\":\"value2\"}"
                + "}";

        testFetchManifestWithResponse(indexJson, "sha256:idx", HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertNotNull(manifest.getAnnotations());
            assertEquals(2, manifest.getAnnotations().size());
            assertEquals("value1", manifest.getAnnotations().get("key1"));
        });
    }

    @Test
    void testFetchManifest_ImageIndex_WithSubject() throws IOException {
        String indexJson = "{"
                + "\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":[{\"digest\":\"sha256:m1\"}],"
                + "\"subject\":{\"digest\":\"sha256:parent\"}"
                + "}";

        testFetchManifestWithResponse(indexJson, "sha256:idx", HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertEquals("sha256:parent", manifest.getSubject());
        });
    }

    @Test
    void testFetchManifest_ImageManifest_WithAnnotationsAndSubject() throws IOException {
        String manifestJson = "{"
                + "\"schemaVersion\":2,"
                + "\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"digest\":\"sha256:lyr\"}],"
                + "\"annotations\":{\"org.opencontainers.image.created\":\"2023-01-01\"},"
                + "\"subject\":{\"digest\":\"sha256:base\"}"
                + "}";

        testFetchManifestWithResponse(manifestJson, "sha256:mani", HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertNotNull(manifest.getAnnotations());
            assertEquals("2023-01-01", manifest.getAnnotations().get("org.opencontainers.image.created"));
            assertEquals("sha256:base", manifest.getSubject());
        });
    }

    @Test
    void testFetchManifest_DetectsIndexByMediaType_Index() throws IOException {
        String indexJson = "{\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\"}";

        testFetchManifestWithResponse(indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
    }

    @Test
    void testFetchManifest_DetectsIndexByMediaType_ManifestList() throws IOException {
        String indexJson = "{\"schemaVersion\":2,"
                + "\"mediaType\":\"application/vnd.docker.distribution.manifest.list.v2+json\"}";

        testFetchManifestWithResponse(indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
    }

    @Test
    void testFetchManifest_DetectsIndexByManifestsField() throws IOException {
        String indexJson = "{\"schemaVersion\":2,\"manifests\":[]}";

        testFetchManifestWithResponse(indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
    }

    @Test
    void testFetchManifest_HttpError_404() throws IOException {
        testFetchManifestWithError(HttpStatus.SC_NOT_FOUND, "HTTP 404");
    }

    @Test
    void testFetchManifest_HttpError_401() throws IOException {
        testFetchManifestWithError(HttpStatus.SC_UNAUTHORIZED, "HTTP 401");
    }

    @Test
    void testFetchManifest_HttpError_500() throws IOException {
        testFetchManifestWithError(HttpStatus.SC_INTERNAL_SERVER_ERROR, "HTTP 500");
    }

    @Test
    void testFetchManifest_EmptyResponseBody() throws IOException {
        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockResponse.getEntity()).thenReturn(null);
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
        when(mockAuthProvider.getAuthUsername()).thenReturn("user");
        when(mockAuthProvider.getAuthToken()).thenReturn("token");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, "latest"));
            assertTrue(exception.getMessage().contains("empty response body"));

            client.close();
        }
    }

    @Test
    void testFetchManifest_ManifestWithoutConfigOrLayers() throws IOException {
        String manifestJson = "{\"schemaVersion\":2}";

        testFetchManifestWithResponse(manifestJson, "sha256:minimal", HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertNull(manifest.getConfigDigest());
            assertNotNull(manifest.getLayerInfos());
            assertTrue(manifest.getLayerInfos().isEmpty());
        });
    }

    @Test
    void testFetchManifest_IndexWithPlatformButNoOsOrArch() throws IOException {
        String indexJson = "{"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":[{\"digest\":\"sha256:m1\",\"platform\":{}}]"
                + "}";

        testFetchManifestWithResponse(indexJson, null, HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertEquals(1, manifest.getIndexManifests().size());
            Manifest.IndexEntry entry = manifest.getIndexManifests().get(0);
            assertNull(entry.getOs());
            assertNull(entry.getArchitecture());
        });
    }

    @Test
    void testFetchManifest_IndexManifestWithoutPlatform() throws IOException {
        String indexJson = "{"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":[{\"digest\":\"sha256:noplat\"}]"
                + "}";

        testFetchManifestWithResponse(indexJson, null, HttpStatus.SC_OK, manifest -> {
            assertTrue(manifest.isIndex());
            assertEquals(1, manifest.getIndexManifests().size());
            assertNull(manifest.getIndexManifests().get(0).getOs());
        });
    }

    @Test
    void testFetchManifest_LayerWithoutMediaTypeOrSize() throws IOException {
        String manifestJson = "{"
                + "\"schemaVersion\":2,"
                + "\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"digest\":\"sha256:lyr\"}]"
                + "}";

        testFetchManifestWithResponse(manifestJson, null, HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertEquals(1, manifest.getLayerInfos().size());
            assertEquals("sha256:lyr", manifest.getLayerInfos().get(0).getDigest());
            assertNull(manifest.getLayerInfos().get(0).getMediaType());
            assertNull(manifest.getLayerInfos().get(0).getSize());
        });
    }

    @Test
    void testFetchManifest_EmptyAnnotationsObject() throws IOException {
        String manifestJson = "{"
                + "\"schemaVersion\":2,"
                + "\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"digest\":\"sha256:lyr\"}],"
                + "\"annotations\":{}"
                + "}";

        testFetchManifestWithResponse(manifestJson, null, HttpStatus.SC_OK, manifest -> {
            assertFalse(manifest.isIndex());
            assertNotNull(manifest.getAnnotations());
            assertTrue(manifest.getAnnotations().isEmpty());
        });
    }

    @Test
    void testFetchManifest_ManifestSizeExceedsLimit() throws IOException {
        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);

        long oversizedLength = 101L * 1024 * 1024; // 101 MB
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContentLength()).thenReturn(oversizedLength);
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
        when(mockAuthProvider.getAuthUsername()).thenReturn("user");
        when(mockAuthProvider.getAuthToken()).thenReturn("token");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, "latest"));
            assertTrue(exception.getMessage().contains("exceeds maximum allowed size"));

            client.close();
        }
    }

    @Test
    void testFetchManifest_DigestMismatch() throws IOException {
        String manifestJson = "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"digest\":\"sha256:lyr\"}]}";
        String requestedDigest = "sha256:expected123";
        String actualDigest = "sha256:actual456";

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);
        Header mockDigestHeader = mock(Header.class);

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContentLength()).thenReturn(100L);
        when(mockEntity.getContent()).thenReturn(
                new ByteArrayInputStream(manifestJson.getBytes(StandardCharsets.UTF_8)));
        when(mockDigestHeader.getValue()).thenReturn(actualDigest);
        when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(mockDigestHeader);
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
        when(mockAuthProvider.getAuthUsername()).thenReturn("user");
        when(mockAuthProvider.getAuthToken()).thenReturn("token");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, requestedDigest));
            assertTrue(exception.getMessage().contains("Manifest digest mismatch"));
            assertTrue(exception.getMessage().contains(requestedDigest));
            assertTrue(exception.getMessage().contains(actualDigest));

            client.close();
        }
    }

    @Test
    void testFetchManifest_InvalidJSON() throws IOException {
        String invalidJson = "{this is not valid json}";

        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);

        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContentLength()).thenReturn(100L);
        when(mockEntity.getContent()).thenReturn(
                new ByteArrayInputStream(invalidJson.getBytes(StandardCharsets.UTF_8)));
        when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(null);
        when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
        when(mockAuthProvider.getAuthUsername()).thenReturn("user");
        when(mockAuthProvider.getAuthToken()).thenReturn("token");

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
            mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                    .thenReturn(basicChallenge);
            mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, "latest"));
            assertTrue(exception.getMessage().contains("Invalid JSON response"));

            client.close();
        }
    }

    @Test
    void testFetchManifest_IndexManifestMissingDigest() throws IOException {
        String invalidIndexJson = "{"
                + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
                + "\"manifests\":[{\"platform\":{\"os\":\"linux\"}}]"  // Missing digest
                + "}";

        testFetchManifestWithErrorExpected(invalidIndexJson, "manifest entry missing required 'digest' field");
    }

    @Test
    void testFetchManifest_LayerMissingDigest() throws IOException {
        String invalidManifestJson = "{"
                + "\"schemaVersion\":2,"
                + "\"config\":{\"digest\":\"sha256:cfg\"},"
                + "\"layers\":[{\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\"}]"  // Missing digest
                + "}";

        testFetchManifestWithErrorExpected(invalidManifestJson, "layer missing required 'digest' field");
    }

    // ========== Helper Methods ==========

    private void testFetchManifestWithResponse(String responseBody, String digestHeader,
                                                int statusCode, ManifestAssertion assertion) throws IOException {
        CloseableHttpClient mockHttpClient = createMockHttpClientForManifest(
                responseBody, digestHeader, statusCode, 100L);

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            Manifest manifest = client.fetchManifest(REPOSITORY, "latest");
            assertNotNull(manifest);
            assertion.assertManifest(manifest);

            client.close();
        }
    }

    private void testFetchManifestWithError(int statusCode, String expectedMessageSubstring) throws IOException {
        CloseableHttpClient mockHttpClient = createMockHttpClientForManifest(
                "error body", null, statusCode, 100L);

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, "latest"));
            assertTrue(exception.getMessage().contains(expectedMessageSubstring),
                    "Expected message containing '" + expectedMessageSubstring + "' but got: " + exception.getMessage());

            client.close();
        }
    }

    private void testFetchManifestWithErrorExpected(String responseBody, String expectedErrorSubstring) 
            throws IOException {
        CloseableHttpClient mockHttpClient = createMockHttpClientForManifest(
                responseBody, null, HttpStatus.SC_OK, 100L);

        try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
            OciRegistryClient client = new OciRegistryClient(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

            IOException exception = assertThrows(IOException.class,
                    () -> client.fetchManifest(REPOSITORY, "latest"));
            assertTrue(exception.getMessage().contains(expectedErrorSubstring),
                    "Expected error containing '" + expectedErrorSubstring + "' but got: " + exception.getMessage());

            client.close();
        }
    }

    private CloseableHttpClient createMockHttpClientForManifest(String responseBody, String digestHeader,
                                                                 int statusCode, long contentLength) {
        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);

        try {
            when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
            when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
            when(mockResponse.getEntity()).thenReturn(mockEntity);
            when(mockEntity.getContentLength()).thenReturn(contentLength);
            when(mockEntity.getContent()).thenReturn(
                    new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));

            if (digestHeader != null) {
                Header mockDigestHeader = mock(Header.class);
                when(mockDigestHeader.getValue()).thenReturn(digestHeader);
                when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(mockDigestHeader);
            } else {
                when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(null);
            }

            when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup mocks", e);
        }

        return mockHttpClient;
    }

    private MockedStatic<AuthChallenge> mockAuthChallenge() {
        AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
        try {
            when(mockAuthProvider.getAuthUsername()).thenReturn("user");
            when(mockAuthProvider.getAuthToken()).thenReturn("token");
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup auth mocks", e);
        }

        MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class);
        mockedAuthChallenge.when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
                .thenReturn(basicChallenge);
        mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();
        return mockedAuthChallenge;
    }

    @FunctionalInterface
    interface ManifestAssertion {
        void assertManifest(com.salesforce.multicloudj.registry.model.Manifest manifest);
    }
}
