package com.salesforce.multicloudj.registry.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.model.Manifest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

/** Unit tests for OciHttpTransport. */
public class OciHttpTransportTest {

  private static final String REGISTRY_ENDPOINT = "https://test-registry.example.com";
  private static final String REPOSITORY = "test-repo/test-image";

  @Mock private AbstractRegistry mockAuthProvider;

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
  void testGetHttpAuthHeader_BasicAuth() throws Exception {
    AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
    String expectedHeader =
        "Basic "
            + Base64.getEncoder()
                .encodeToString("testuser:testtoken".getBytes(StandardCharsets.UTF_8));

    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn(expectedHeader);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();
      mockedAuthChallenge.when(AuthChallenge::anonymous).thenCallRealMethod();

      OciHttpTransport transport = new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider);

      String authHeader = transport.getHttpAuthHeader(REPOSITORY);

      assertNotNull(authHeader);
      assertEquals(expectedHeader, authHeader);

      transport.close();
    }
  }

  @Test
  void testGetHttpAuthHeader_BearerAuth() throws Exception {
    AuthChallenge bearerChallenge =
        AuthChallenge.parse(
            "Bearer realm=\"https://auth.example.com/token\",service=\"registry.example.com\"");
    String expectedHeader = "Bearer bearer-token-123";

    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn(expectedHeader);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(bearerChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport = new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider);

      String authHeader = transport.getHttpAuthHeader(REPOSITORY);

      assertNotNull(authHeader);
      assertEquals(expectedHeader, authHeader);

      transport.close();
    }
  }

  @Test
  void testGetHttpAuthHeader_AnonymousAuth() throws Exception {
    AuthChallenge anonymousChallenge = AuthChallenge.anonymous();

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(anonymousChallenge);
      mockedAuthChallenge.when(AuthChallenge::anonymous).thenCallRealMethod();

      OciHttpTransport transport = new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider);

      String authHeader = transport.getHttpAuthHeader(REPOSITORY);

      assertNull(authHeader);

      transport.close();
    }
  }

  @Test
  void testGetHttpAuthHeader_CachesChallenge() throws Exception {
    AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");

    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn("Basic dXNlcjp0b2tlbg==");

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport = new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider);

      // Call twice
      transport.getHttpAuthHeader(REPOSITORY);
      transport.getHttpAuthHeader(REPOSITORY);

      // Verify discover was called only once (challenge is cached)
      mockedAuthChallenge.verify(
          () -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()), times(1));

      transport.close();
    }
  }

  @Test
  void testFetchManifest_SingleImageManifest_WithDockerDigestHeader() throws Exception {
    String manifestJson =
        "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\","
            + "\"config\":{\"digest\":\"sha256:config123\",\"size\":1234},\"layers\":["
            + "{\"digest\":\"sha256:layer1\","
            + "\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"size\":5678},"
            + "{\"digest\":\"sha256:layer2\","
            + "\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"size\":9012}"
            + "]}";

    testFetchManifestWithResponse(
        manifestJson,
        "sha256:manifestdigest",
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertEquals("sha256:manifestdigest", manifest.getDigest());
          assertEquals("sha256:config123", manifest.getConfigDigest());
          assertNotNull(manifest.getLayerInfos());
          assertEquals(2, manifest.getLayerInfos().size());
          assertEquals("sha256:layer1", manifest.getLayerInfos().get(0).getDigest());
          assertEquals(
              "application/vnd.oci.image.layer.v1.tar+gzip",
              manifest.getLayerInfos().get(0).getMediaType());
          assertEquals(5678L, manifest.getLayerInfos().get(0).getSize());
        });
  }

  @Test
  void testFetchManifest_SingleImageManifest_WithoutDockerDigestHeader() throws Exception {
    String manifestJson =
        "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:cfg\"},"
            + "\"layers\":[{\"digest\":\"sha256:lyr\"}]}";

    testFetchManifestWithResponse(
        manifestJson,
        null,
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertNotNull(manifest.getDigest());
          assertTrue(manifest.getDigest().startsWith("sha256:"));
          assertEquals("sha256:cfg", manifest.getConfigDigest());
        });
  }

  @Test
  void testFetchManifest_ImageIndex_WithManifestsArray() throws Exception {
    String indexJson =
        "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":["
            + "{\"digest\":\"sha256:amd64manifest\","
            + "\"platform\":{\"os\":\"linux\",\"architecture\":\"amd64\"}},"
            + "{\"digest\":\"sha256:arm64manifest\","
            + "\"platform\":{\"os\":\"linux\",\"architecture\":\"arm64\"}}"
            + "]}";

    testFetchManifestWithResponse(
        indexJson,
        "sha256:indexdigest",
        HttpStatus.SC_OK,
        manifest -> {
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
  void testFetchManifest_ImageIndex_WithEnhancedPlatformFields() throws Exception {
    String indexJson =
        "{"
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

    testFetchManifestWithResponse(
        indexJson,
        "sha256:idx",
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertEquals(1, manifest.getIndexManifests().size());
          Manifest.IndexEntry entry = manifest.getIndexManifests().get(0);
          assertEquals("windows", entry.getOs());
          assertEquals("amd64", entry.getArchitecture());
        });
  }

  @Test
  void testFetchManifest_ImageIndex_NullManifestsArray() throws Exception {
    String indexJson =
        "{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.index.v1+json\"}";

    testFetchManifestWithResponse(
        indexJson,
        "sha256:emptyindex",
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertNotNull(manifest.getIndexManifests());
          assertTrue(manifest.getIndexManifests().isEmpty());
        });
  }

  @Test
  void testFetchManifest_ImageIndex_WithAnnotations() throws Exception {
    String indexJson =
        "{"
            + "\"schemaVersion\":2,"
            + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":[{\"digest\":\"sha256:m1\"}],"
            + "\"annotations\":{\"key1\":\"value1\",\"key2\":\"value2\"}"
            + "}";

    testFetchManifestWithResponse(
        indexJson,
        "sha256:idx",
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertNotNull(manifest.getAnnotations());
          assertEquals(2, manifest.getAnnotations().size());
          assertEquals("value1", manifest.getAnnotations().get("key1"));
        });
  }

  @Test
  void testFetchManifest_ImageIndex_WithSubject() throws Exception {
    String indexJson =
        "{"
            + "\"schemaVersion\":2,"
            + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":[{\"digest\":\"sha256:m1\"}],"
            + "\"subject\":{\"digest\":\"sha256:parent\"}"
            + "}";

    testFetchManifestWithResponse(
        indexJson,
        "sha256:idx",
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertEquals("sha256:parent", manifest.getSubject());
        });
  }

  @Test
  void testFetchManifest_ImageManifest_WithAnnotationsAndSubject() throws Exception {
    String manifestJson =
        "{"
            + "\"schemaVersion\":2,"
            + "\"config\":{\"digest\":\"sha256:cfg\"},"
            + "\"layers\":[{\"digest\":\"sha256:lyr\"}],"
            + "\"annotations\":{\"org.opencontainers.image.created\":\"2023-01-01\"},"
            + "\"subject\":{\"digest\":\"sha256:base\"}"
            + "}";

    testFetchManifestWithResponse(
        manifestJson,
        "sha256:mani",
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertNotNull(manifest.getAnnotations());
          assertEquals(
              "2023-01-01", manifest.getAnnotations().get("org.opencontainers.image.created"));
          assertEquals("sha256:base", manifest.getSubject());
        });
  }

  @Test
  void testFetchManifest_DetectsIndexByMediaType_Index() throws Exception {
    String indexJson =
        "{\"schemaVersion\":2," + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\"}";

    testFetchManifestWithResponse(
        indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
  }

  @Test
  void testFetchManifest_DetectsIndexByMediaType_ManifestList() throws Exception {
    String indexJson =
        "{\"schemaVersion\":2,"
            + "\"mediaType\":\"application/vnd.docker.distribution.manifest.list.v2+json\"}";

    testFetchManifestWithResponse(
        indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
  }

  @Test
  void testFetchManifest_DetectsIndexByManifestsField() throws Exception {
    String indexJson = "{\"schemaVersion\":2,\"manifests\":[]}";

    testFetchManifestWithResponse(
        indexJson, null, HttpStatus.SC_OK, manifest -> assertTrue(manifest.isIndex()));
  }

  @Test
  void testFetchManifest_HttpError_404() throws Exception {
    testFetchManifestWithError(
        HttpStatus.SC_NOT_FOUND, "HTTP 404", ResourceNotFoundException.class);
  }

  @Test
  void testFetchManifest_HttpError_401() throws Exception {
    testFetchManifestWithError(HttpStatus.SC_UNAUTHORIZED, "HTTP 401", UnAuthorizedException.class);
  }

  @Test
  void testFetchManifest_HttpError_500() throws Exception {
    testFetchManifestWithError(
        HttpStatus.SC_INTERNAL_SERVER_ERROR, "HTTP 500", UnknownException.class);
  }

  @Test
  void testFetchManifest_EmptyResponseBody() throws Exception {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(null);
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

    AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn("Basic dXNlcjp0b2tlbg==");

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(UnknownException.class, () -> transport.fetchManifest(REPOSITORY, "latest"));
      assertTrue(exception.getMessage().contains("empty response body"));

      transport.close();
    }
  }

  @Test
  void testFetchManifest_ManifestWithoutConfigOrLayers() throws Exception {
    String manifestJson = "{\"schemaVersion\":2}";

    testFetchManifestWithResponse(
        manifestJson,
        "sha256:minimal",
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertNull(manifest.getConfigDigest());
          assertNotNull(manifest.getLayerInfos());
          assertTrue(manifest.getLayerInfos().isEmpty());
        });
  }

  @Test
  void testFetchManifest_IndexWithPlatformButNoOsOrArch() throws Exception {
    String indexJson =
        "{"
            + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":[{\"digest\":\"sha256:m1\",\"platform\":{}}]"
            + "}";

    testFetchManifestWithResponse(
        indexJson,
        null,
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertEquals(1, manifest.getIndexManifests().size());
          Manifest.IndexEntry entry = manifest.getIndexManifests().get(0);
          assertNull(entry.getOs());
          assertNull(entry.getArchitecture());
        });
  }

  @Test
  void testFetchManifest_IndexManifestWithoutPlatform() throws Exception {
    String indexJson =
        "{"
            + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":[{\"digest\":\"sha256:noplat\"}]"
            + "}";

    testFetchManifestWithResponse(
        indexJson,
        null,
        HttpStatus.SC_OK,
        manifest -> {
          assertTrue(manifest.isIndex());
          assertEquals(1, manifest.getIndexManifests().size());
          assertNull(manifest.getIndexManifests().get(0).getOs());
        });
  }

  @Test
  void testFetchManifest_LayerWithoutMediaTypeOrSize() throws Exception {
    String manifestJson =
        "{"
            + "\"schemaVersion\":2,"
            + "\"config\":{\"digest\":\"sha256:cfg\"},"
            + "\"layers\":[{\"digest\":\"sha256:lyr\"}]"
            + "}";

    testFetchManifestWithResponse(
        manifestJson,
        null,
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertEquals(1, manifest.getLayerInfos().size());
          assertEquals("sha256:lyr", manifest.getLayerInfos().get(0).getDigest());
          assertNull(manifest.getLayerInfos().get(0).getMediaType());
          assertNull(manifest.getLayerInfos().get(0).getSize());
        });
  }

  @Test
  void testFetchManifest_EmptyAnnotationsObject() throws Exception {
    String manifestJson =
        "{"
            + "\"schemaVersion\":2,"
            + "\"config\":{\"digest\":\"sha256:cfg\"},"
            + "\"layers\":[{\"digest\":\"sha256:lyr\"}],"
            + "\"annotations\":{}"
            + "}";

    testFetchManifestWithResponse(
        manifestJson,
        null,
        HttpStatus.SC_OK,
        manifest -> {
          assertFalse(manifest.isIndex());
          assertNotNull(manifest.getAnnotations());
          assertTrue(manifest.getAnnotations().isEmpty());
        });
  }

  @Test
  void testFetchManifest_ManifestSizeExceedsLimit() throws Exception {
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
    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn("Basic dXNlcjp0b2tlbg==");

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(UnknownException.class, () -> transport.fetchManifest(REPOSITORY, "latest"));
      assertTrue(exception.getMessage().contains("exceeds maximum allowed size"));

      transport.close();
    }
  }

  @Test
  void testFetchManifest_DigestMismatch() throws Exception {
    String manifestJson =
        "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:cfg\"},"
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
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(manifestJson.getBytes(StandardCharsets.UTF_8)));
    when(mockDigestHeader.getValue()).thenReturn(actualDigest);
    when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(mockDigestHeader);
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

    AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn("Basic dXNlcjp0b2tlbg==");

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(
              UnknownException.class, () -> transport.fetchManifest(REPOSITORY, requestedDigest));
      assertTrue(exception.getMessage().contains("Manifest digest mismatch"));
      assertTrue(exception.getMessage().contains(requestedDigest));
      assertTrue(exception.getMessage().contains(actualDigest));

      transport.close();
    }
  }

  @Test
  void testFetchManifest_InvalidJSON() throws Exception {
    String invalidJson = "{this is not valid json}";

    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContentLength()).thenReturn(100L);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(invalidJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getFirstHeader("Docker-Content-Digest")).thenReturn(null);
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

    AuthChallenge basicChallenge = AuthChallenge.parse("Basic realm=\"test\"");
    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn("Basic dXNlcjp0b2tlbg==");

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(basicChallenge);
      mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();

      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(UnknownException.class, () -> transport.fetchManifest(REPOSITORY, "latest"));
      assertTrue(exception.getMessage().contains("Invalid JSON response"));

      transport.close();
    }
  }

  @Test
  void testFetchManifest_IndexManifestMissingDigest() throws Exception {
    String invalidIndexJson =
        "{"
            + "\"mediaType\":\"application/vnd.oci.image.index.v1+json\","
            + "\"manifests\":[{\"platform\":{\"os\":\"linux\"}}]"
            + "}";

    testFetchManifestWithErrorExpected(
        invalidIndexJson, "manifest entry missing required 'digest' field");
  }

  @Test
  void testFetchManifest_LayerMissingDigest() throws Exception {
    String invalidManifestJson =
        "{\"schemaVersion\":2,\"config\":{\"digest\":\"sha256:cfg\"},"
            + "\"layers\":[{\"mediaType\":"
            + "\"application/vnd.oci.image.layer.v1.tar+gzip\"}]"
            + "}";

    testFetchManifestWithErrorExpected(
        invalidManifestJson, "layer missing required 'digest' field");
  }

  @Test
  void testDownloadBlob_Success() throws Exception {
    String blobContent = "test blob content";
    String digest = "sha256:abc123";

    CloseableHttpClient mockHttpClient =
        createMockHttpClientForBlob(blobContent, HttpStatus.SC_OK, true);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      try (InputStream stream = transport.downloadBlob(REPOSITORY, digest)) {
        assertNotNull(stream);
        byte[] content = stream.readAllBytes();
        assertEquals(blobContent, new String(content, StandardCharsets.UTF_8));
      }

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_HttpError_404_ThrowsResourceNotFoundException() throws Exception {
    String digest = "sha256:notfound";

    CloseableHttpClient mockHttpClient =
        createMockHttpClientForBlob("Not Found", HttpStatus.SC_NOT_FOUND, true);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      ResourceNotFoundException exception =
          assertThrows(
              ResourceNotFoundException.class, () -> transport.downloadBlob(REPOSITORY, digest));
      assertTrue(exception.getMessage().contains("HTTP 404"));

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_HttpError_401_ThrowsUnAuthorizedException() throws Exception {
    String digest = "sha256:unauthorized";

    CloseableHttpClient mockHttpClient =
        createMockHttpClientForBlob("Unauthorized", HttpStatus.SC_UNAUTHORIZED, true);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnAuthorizedException exception =
          assertThrows(
              UnAuthorizedException.class, () -> transport.downloadBlob(REPOSITORY, digest));
      assertTrue(exception.getMessage().contains("HTTP 401"));

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_HttpError_403_ThrowsUnAuthorizedException() throws Exception {
    String digest = "sha256:forbidden";

    CloseableHttpClient mockHttpClient =
        createMockHttpClientForBlob("Forbidden", HttpStatus.SC_FORBIDDEN, true);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnAuthorizedException exception =
          assertThrows(
              UnAuthorizedException.class, () -> transport.downloadBlob(REPOSITORY, digest));
      assertTrue(exception.getMessage().contains("HTTP 403"));

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_HttpError_500_ThrowsUnknownException() throws Exception {
    String digest = "sha256:servererror";

    CloseableHttpClient mockHttpClient =
        createMockHttpClientForBlob(
            "Internal Server Error", HttpStatus.SC_INTERNAL_SERVER_ERROR, true);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(UnknownException.class, () -> transport.downloadBlob(REPOSITORY, digest));
      assertTrue(exception.getMessage().contains("HTTP 500"));

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_EmptyResponseBody_ThrowsUnknownException() throws Exception {
    String digest = "sha256:empty";

    CloseableHttpClient mockHttpClient = createMockHttpClientForBlob(null, HttpStatus.SC_OK, false);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      UnknownException exception =
          assertThrows(UnknownException.class, () -> transport.downloadBlob(REPOSITORY, digest));
      assertTrue(exception.getMessage().contains("empty response body"));

      transport.close();
    }
  }

  @Test
  void testDownloadBlob_SetsAuthorizationHeader() throws Exception {
    String digest = "sha256:authtest";
    CloseableHttpClient mockHttpClient =
        createMockHttpClientWithExecuteAnswer(
            "auth test",
            invocation -> {
              HttpGet request = invocation.getArgument(0);
              Header authHeader = request.getFirstHeader("Authorization");
              assertNotNull(authHeader, "Authorization header should be set");
              assertEquals(
                  "Basic "
                      + Base64.getEncoder()
                          .encodeToString("user:token".getBytes(StandardCharsets.UTF_8)),
                  authHeader.getValue());
            });

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);
      try (InputStream stream = transport.downloadBlob(REPOSITORY, digest)) {
        assertNotNull(stream);
      }
      transport.close();
    }
  }

  @Test
  void testDownloadBlob_DoesNotSetAuthHeaderForAnonymous() throws Exception {
    String digest = "sha256:noauthtest";
    CloseableHttpClient mockHttpClient =
        createMockHttpClientWithExecuteAnswer(
            "no auth test",
            invocation -> {
              HttpGet request = invocation.getArgument(0);
              Header authHeader = request.getFirstHeader("Authorization");
              assertNull(authHeader, "Authorization header should not be set for anonymous auth");
            });

    AuthChallenge anonymousChallenge = AuthChallenge.anonymous();
    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class)) {
      mockedAuthChallenge
          .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
          .thenReturn(anonymousChallenge);
      mockedAuthChallenge.when(AuthChallenge::anonymous).thenCallRealMethod();

      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);
      try (InputStream stream = transport.downloadBlob(REPOSITORY, digest)) {
        assertNotNull(stream);
      }
      transport.close();
    }
  }

  private void testFetchManifestWithResponse(
      String responseBody, String digestHeader, int statusCode, ManifestAssertion assertion)
      throws Exception {
    CloseableHttpClient mockHttpClient =
        createMockHttpClientForManifest(responseBody, digestHeader, statusCode, 100L);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      Manifest manifest = transport.fetchManifest(REPOSITORY, "latest");
      assertNotNull(manifest);
      assertion.assertManifest(manifest);

      transport.close();
    }
  }

  private <E extends Exception> void testFetchManifestWithError(
      int statusCode, String expectedMessageSubstring, Class<E> expectedExceptionType)
      throws Exception {
    CloseableHttpClient mockHttpClient =
        createMockHttpClientForManifest("error body", null, statusCode, 100L);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      Exception exception =
          assertThrows(expectedExceptionType, () -> transport.fetchManifest(REPOSITORY, "latest"));
      assertTrue(
          exception.getMessage().contains(expectedMessageSubstring),
          "Expected message containing '"
              + expectedMessageSubstring
              + "' but got: "
              + exception.getMessage());

      transport.close();
    }
  }

  private void testFetchManifestWithErrorExpected(
      String responseBody, String expectedErrorSubstring) throws Exception {
    CloseableHttpClient mockHttpClient =
        createMockHttpClientForManifest(responseBody, null, HttpStatus.SC_OK, 100L);

    try (MockedStatic<AuthChallenge> mockedAuthChallenge = mockAuthChallenge()) {
      OciHttpTransport transport =
          new OciHttpTransport(REGISTRY_ENDPOINT, mockAuthProvider, mockHttpClient);

      InvalidArgumentException exception =
          assertThrows(
              InvalidArgumentException.class, () -> transport.fetchManifest(REPOSITORY, "latest"));
      assertTrue(
          exception.getMessage().contains(expectedErrorSubstring),
          "Expected error containing '"
              + expectedErrorSubstring
              + "' but got: "
              + exception.getMessage());

      transport.close();
    }
  }

  private CloseableHttpClient createMockHttpClientForManifest(
      String responseBody, String digestHeader, int statusCode, long contentLength) {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    try {
      when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockEntity.getContentLength()).thenReturn(contentLength);
      when(mockEntity.getContent())
          .thenReturn(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));

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
    when(mockAuthProvider.getAuthorizationHeader(any(), anyString()))
        .thenReturn(
            "Basic "
                + Base64.getEncoder()
                    .encodeToString("user:token".getBytes(StandardCharsets.UTF_8)));

    MockedStatic<AuthChallenge> mockedAuthChallenge = mockStatic(AuthChallenge.class);
    mockedAuthChallenge
        .when(() -> AuthChallenge.discover(any(CloseableHttpClient.class), anyString()))
        .thenReturn(basicChallenge);
    mockedAuthChallenge.when(() -> AuthChallenge.parse(anyString())).thenCallRealMethod();
    return mockedAuthChallenge;
  }

  private CloseableHttpClient createMockHttpClientWithExecuteAnswer(
      String blobContent, Consumer<InvocationOnMock> requestAssertion) {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);
    try {
      when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockEntity.getContent())
          .thenReturn(new ByteArrayInputStream(blobContent.getBytes(StandardCharsets.UTF_8)));
      when(mockHttpClient.execute(any(HttpGet.class)))
          .thenAnswer(
              invocation -> {
                requestAssertion.accept(invocation);
                return mockResponse;
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup mock", e);
    }
    return mockHttpClient;
  }

  private CloseableHttpClient createMockHttpClientForBlob(
      String blobContent, int statusCode, boolean hasEntity) {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = hasEntity ? mock(HttpEntity.class) : null;

    try {
      when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockResponse.getEntity()).thenReturn(mockEntity);

      if (hasEntity && blobContent != null) {
        when(mockEntity.getContent())
            .thenAnswer(
                invocation ->
                    new ByteArrayInputStream(blobContent.getBytes(StandardCharsets.UTF_8)));
      }

      when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup blob mocks", e);
    }

    return mockHttpClient;
  }

  @FunctionalInterface
  interface ManifestAssertion {
    void assertManifest(Manifest manifest);
  }
}
