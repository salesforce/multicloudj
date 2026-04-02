package com.salesforce.multicloudj.registry.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpCredentialsProvider;
import com.salesforce.multicloudj.registry.driver.AuthChallenge;
import com.salesforce.multicloudj.registry.driver.BearerTokenExchange;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.IOException;
import java.util.Date;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class GcpRegistryTest {

  private static final String TEST_REGISTRY_ENDPOINT = "https://us-central1-docker.pkg.dev";
  private static final String TEST_REGISTRY_HOST = "us-central1-docker.pkg.dev";
  private static final String GCP_TOKEN_URL = "https://oauth2.googleapis.com/token";
  private static final String PROVIDER_ID = "gcp";
  private static final String TEST_REPOSITORY = "my-repo";
  private static final String TEST_REPOSITORY_WITH_IMAGE = "my-repo/my-image";
  private static final AuthChallenge BEARER_CHALLENGE =
      AuthChallenge.parse(
          "Bearer realm=\"" + GCP_TOKEN_URL + "\",service=\"" + TEST_REGISTRY_HOST + "\"");

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

      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
      try {
        action.execute(registry);
      } finally {
        registry.close();
      }
    }
  }

  @Test
  void testBuilderAndBasicProperties() throws Exception {
    withMockedRegistry(
        registry -> {
          assertNotNull(registry);
          assertEquals(PROVIDER_ID, registry.getProviderId());
          assertNotNull(registry.getOciTransport());
          assertNotNull(registry.builder());
        });
  }

  @Test
  void testNoArgConstructor_ForServiceLoaderDiscovery() {
    GcpRegistry registry = new GcpRegistry();
    assertEquals(PROVIDER_ID, registry.getProviderId());
    assertNotNull(registry.builder());
  }

  @Test
  void testGetAuthorizationHeader_PassesIdentityTokenToBearerExchange() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      AccessToken accessToken =
          new AccessToken("test-token-value", new Date(System.currentTimeMillis() + 3600000));

      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      BearerTokenExchange mockTokenExchange = mock(BearerTokenExchange.class);
      when(mockTokenExchange.getBearerToken(
              any(), eq("test-token-value"), anyString(), anyString()))
          .thenReturn("registry-bearer-token");

      GcpRegistry registry =
          new GcpRegistry(
              new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT),
              null,
              null,
              mockTokenExchange);

      String header = registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY);
      assertEquals("Bearer registry-bearer-token", header);

      registry.close();
    }
  }

  @Test
  void testGetAuthorizationHeader_NullAccessToken_ThrowsUnknownException() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);

      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      when(scopedCredentials.getAccessToken()).thenReturn(null);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();

      UnknownException exception =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY));
      assertEquals(
          "Failed to obtain GCP access token: access token is null", exception.getMessage());
      registry.close();
    }
  }

  @Test
  void testGetAuthorizationHeader_NullTokenValue_ThrowsUnknownException() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      AccessToken accessToken = mock(AccessToken.class);

      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
      when(accessToken.getTokenValue()).thenReturn(null);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();

      UnknownException exception =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY));
      assertEquals(
          "Failed to obtain GCP access token: token value is null", exception.getMessage());
      registry.close();
    }
  }

  @Test
  void testGetException_WithSubstrateSdkException() throws Exception {
    withMockedRegistry(
        registry -> {
          SubstrateSdkException testException = new SubstrateSdkException("Test");
          assertEquals(SubstrateSdkException.class, registry.getException(testException));
        });
  }

  @Test
  void testGetException_WithApiException() throws Exception {
    withMockedRegistry(
        registry -> {
          StatusCode mockStatusCode = mock(StatusCode.class);
          when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
          ApiException apiException = mock(ApiException.class);
          when(apiException.getStatusCode()).thenReturn(mockStatusCode);

          assertEquals(ResourceNotFoundException.class, registry.getException(apiException));
        });
  }

  @Test
  void testGetException_WithIllegalArgumentException() throws Exception {
    withMockedRegistry(
        registry -> {
          assertEquals(
              InvalidArgumentException.class,
              registry.getException(new IllegalArgumentException("Invalid")));
        });
  }

  @Test
  void testGetException_WithUnknownException() throws Exception {
    withMockedRegistry(
        registry -> {
          assertEquals(UnknownException.class, registry.getException(new RuntimeException("Test")));
        });
  }

  @Test
  void testGetAuthorizationHeader_TokenCachingCallsApplicationDefaultOnce() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      AccessToken accessToken =
          new AccessToken("cached-token", new Date(System.currentTimeMillis() + 3600000));

      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      BearerTokenExchange mockTokenExchange = mock(BearerTokenExchange.class);
      when(mockTokenExchange.getBearerToken(any(), anyString(), anyString(), anyString()))
          .thenReturn("bearer-token");

      GcpRegistry registry =
          new GcpRegistry(
              new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT),
              null,
              null,
              mockTokenExchange);

      registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY);
      registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY);

      // getApplicationDefault should only be called once — credentials are cached
      mockedStatic.verify(GoogleCredentials::getApplicationDefault, times(1));

      registry.close();
    }
  }

  @Test
  void testGetAuthorizationHeader_ReturnsBearerHeader() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      AccessToken accessToken =
          new AccessToken("gcp-identity-token", new Date(System.currentTimeMillis() + 3600000));

      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      when(scopedCredentials.getAccessToken()).thenReturn(accessToken);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      BearerTokenExchange mockTokenExchange = mock(BearerTokenExchange.class);
      when(mockTokenExchange.getBearerToken(any(), anyString(), anyString(), any()))
          .thenReturn("registry-bearer-token");

      GcpRegistry registry =
          new GcpRegistry(
              new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT),
              null,
              null,
              mockTokenExchange);

      String header = registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY_WITH_IMAGE);

      assertNotNull(header);
      assertEquals("Bearer registry-bearer-token", header);

      registry.close();
    }
  }

  @Test
  void testClose_ClosesTokenHttpClientAndOciClient() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class);
        MockedStatic<HttpClients> mockedHttpClients = mockStatic(HttpClients.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      CloseableHttpClient mockTokenClient = mock(CloseableHttpClient.class);
      mockedHttpClients.when(HttpClients::createDefault).thenReturn(mockTokenClient);
      // OciHttpTransport uses HttpClients.custom() — delegate to the real implementation
      mockedHttpClients.when(HttpClients::custom).thenCallRealMethod();

      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
      registry.close();

      verify(mockTokenClient).close();
    }
  }

  @Test
  void testBuilder_BlankRegistryEndpoint_ThrowsException() {
    assertThrows(InvalidArgumentException.class, () -> new GcpRegistry.Builder().build());
    assertThrows(
        InvalidArgumentException.class,
        () -> new GcpRegistry.Builder().withRegistryEndpoint("").build());
    assertThrows(
        InvalidArgumentException.class,
        () -> new GcpRegistry.Builder().withRegistryEndpoint("   ").build());
  }

  @Test
  void testBuilder_WithPlatform() throws Exception {
    try (MockedStatic<GoogleCredentials> mockedStatic = mockStatic(GoogleCredentials.class)) {
      GoogleCredentials mockCredentials = mock(GoogleCredentials.class);
      GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
      when(mockCredentials.createScoped(anyList())).thenReturn(scopedCredentials);
      mockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);

      Platform customPlatform =
          Platform.builder().operatingSystem("linux").architecture("arm64").build();

      GcpRegistry registry =
          new GcpRegistry.Builder()
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
    withMockedRegistry(
        registry -> {
          assertNotNull(registry.getTargetPlatform());
          assertEquals("linux", registry.getTargetPlatform().getOperatingSystem());
          assertEquals("amd64", registry.getTargetPlatform().getArchitecture());
        });
  }

  @Test
  void testCredentialsOverrider_ReturnsNull_ThrowsException() throws Exception {
    try (MockedStatic<GcpCredentialsProvider> mockedProvider =
        mockStatic(GcpCredentialsProvider.class)) {

      mockedProvider
          .when(() -> GcpCredentialsProvider.getCredentials(any()))
          .thenReturn(null);

      CredentialsOverrider overrider = mock(CredentialsOverrider.class);

      GcpRegistry registry =
          new GcpRegistry.Builder()
              .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
              .withCredentialsOverrider(overrider)
              .build();

      UnknownException exception =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY));
      assertEquals(
          "Failed to obtain credentials from CredentialsOverrider", exception.getMessage());

      registry.close();
    }
  }

  @Test
  void testCreateGoogleCredentials_WithCredentialsOverrider_Success() throws Exception {
    try (MockedStatic<GcpCredentialsProvider> mp = mockStatic(GcpCredentialsProvider.class)) {
      GoogleCredentials creds = mock(GoogleCredentials.class);
      GoogleCredentials scoped = mock(GoogleCredentials.class);
      when(creds.createScoped(anyList())).thenReturn(scoped);
      when(scoped.getAccessToken())
          .thenReturn(
              new AccessToken("overrider-token", new Date(System.currentTimeMillis() + 3600000)));
      mp.when(() -> GcpCredentialsProvider.getCredentials(any())).thenReturn(creds);

      BearerTokenExchange mockTokenExchange = mock(BearerTokenExchange.class);
      when(mockTokenExchange.getBearerToken(any(), eq("overrider-token"), anyString(), anyString()))
          .thenReturn("bearer-token");

      GcpRegistry registry =
          new GcpRegistry(
              new GcpRegistry.Builder()
                  .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                  .withCredentialsOverrider(mock(CredentialsOverrider.class)),
              null,
              null,
              mockTokenExchange);

      String header = registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY);
      assertEquals("Bearer bearer-token", header);
      verify(mockTokenExchange)
          .getBearerToken(any(), eq("overrider-token"), anyString(), anyString());
    }
  }

  @Test
  void testCreateGoogleCredentials_ApplicationDefaultNull_ThrowsUnknownException()
      throws Exception {
    try (MockedStatic<GoogleCredentials> m = mockStatic(GoogleCredentials.class)) {
      m.when(GoogleCredentials::getApplicationDefault).thenReturn(null);
      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
      assertTrue(
          assertThrows(
                  UnknownException.class,
                  () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY))
              .getMessage()
              .contains("application default credentials not available"));
    }
  }

  @Test
  void testCreateGoogleCredentials_IOExceptionWrappedInUnknownException() throws Exception {
    try (MockedStatic<GoogleCredentials> m = mockStatic(GoogleCredentials.class)) {
      m.when(GoogleCredentials::getApplicationDefault).thenThrow(new IOException("fail"));
      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
      assertTrue(
          assertThrows(
                  UnknownException.class,
                  () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY))
              .getMessage()
              .contains("Failed to load GCP credentials"));
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

      GcpRegistry registry =
          new GcpRegistry.Builder().withRegistryEndpoint(TEST_REGISTRY_ENDPOINT).build();
      assertTrue(
          assertThrows(
                  UnknownException.class,
                  () -> registry.getAuthorizationHeader(BEARER_CHALLENGE, TEST_REPOSITORY))
              .getMessage()
              .contains("Failed to refresh GCP credentials"));
    }
  }
}
