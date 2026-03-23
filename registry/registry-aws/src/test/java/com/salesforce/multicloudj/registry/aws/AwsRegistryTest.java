package com.salesforce.multicloudj.registry.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.driver.AuthChallenge;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.http.HttpRequestInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

class AwsRegistryTest {

  private static final String TEST_REGION = "us-east-1";
  private static final String TEST_REGISTRY_ENDPOINT =
      "https://123456789012.dkr.ecr.us-east-1.amazonaws.com";
  private static final String PROVIDER_ID = "aws";
  private static final String AUTH_USERNAME = "AWS";
  private static final String TOKEN_PREFIX = "AWS:";
  private static final String ERR_EMPTY_AUTH_DATA = "ECR returned empty authorization data";
  private static final String ERR_INVALID_TOKEN_FORMAT = "Invalid ECR authorization token format";
  private static final String ERR_FAILED_AUTH_TOKEN = "Failed to get ECR authorization token";
  private static final String AWS_ERROR_CODE_ACCESS_DENIED = "AccessDenied";
  private static final String AWS_ERROR_CODE_UNAVAILABLE = "ServiceUnavailableException";
  private static final long TOKEN_VALIDITY_SECONDS = 43200; // 12 hours
  private static final AuthChallenge BASIC_CHALLENGE = AuthChallenge.parse("Basic realm=\"ecr\"");

  @FunctionalInterface
  interface RegistryTestAction {
    void execute(AwsRegistry registry) throws Exception;
  }

  private AwsRegistry createRegistryWithMockEcrClient(EcrClient mockEcrClient) {
    AwsRegistry.Builder builder =
        new AwsRegistry.Builder()
            .withRegion(TEST_REGION)
            .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT);
    return new AwsRegistry(builder, mockEcrClient);
  }

  private String encodeToken(String token) {
    return Base64.getEncoder().encodeToString((TOKEN_PREFIX + token).getBytes());
  }

  private AuthorizationData authDataWithExpiry(String token, Instant expiresAt) {
    return AuthorizationData.builder()
        .authorizationToken(encodeToken(token))
        .expiresAt(expiresAt)
        .build();
  }

  private GetAuthorizationTokenResponse tokenResponse(AuthorizationData authData) {
    return GetAuthorizationTokenResponse.builder()
        .authorizationData(Collections.singletonList(authData))
        .build();
  }

  private static AwsServiceException awsException(String errorCode) {
    return AwsServiceException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
        .build();
  }

  private String decodeTokenFromBasicHeader(String header) {
    assertTrue(header.startsWith("Basic "));
    String decoded =
        new String(
            Base64.getDecoder().decode(header.substring("Basic ".length())),
            StandardCharsets.UTF_8);
    return decoded.substring(AUTH_USERNAME.length() + 1); // strip "AWS:" prefix
  }

  private void withMockedRegistry(RegistryTestAction action) throws Exception {
    EcrClient mockEcrClient = mock(EcrClient.class);
    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      action.execute(registry);
    }
  }

  @Test
  void testNoArgConstructor_CreatesInstanceWithDefaultBuilder() {
    AwsRegistry registry = new AwsRegistry();
    assertNotNull(registry);
    assertNull(registry.getOciTransport());
  }

  @Test
  void testConstructor_WithBuilder_InitialisesFields() throws Exception {
    withMockedRegistry(
        registry -> {
          assertEquals(PROVIDER_ID, registry.getProviderId());
          assertNotNull(registry.getOciTransport());
        });
  }

  @Test
  void testBuilder_InstanceMethod_ReturnsNewBuilder() throws Exception {
    withMockedRegistry(registry -> assertNotNull(registry.builder()));
  }

  @Test
  void testBuilder_WithRegion_GetRegion_RoundTrip() {
    AwsRegistry.Builder builder = new AwsRegistry.Builder().withRegion(TEST_REGION);
    assertEquals(TEST_REGION, builder.getRegion());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"   "})
  void testBuilder_MissingEndpoint_ThrowsInvalidArgumentException(String endpoint) {
    assertThrows(
        InvalidArgumentException.class,
        () ->
            new AwsRegistry.Builder()
                .withRegion(TEST_REGION)
                .withRegistryEndpoint(endpoint)
                .build());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"   "})
  void testBuilder_MissingRegion_ThrowsInvalidArgumentException(String region) {
    assertThrows(
        InvalidArgumentException.class,
        () ->
            new AwsRegistry.Builder()
                .withRegion(region)
                .withRegistryEndpoint(TEST_REGISTRY_ENDPOINT)
                .build());
  }

  @Test
  void testGetInterceptors_ReturnsAuthStrippingInterceptor() throws Exception {
    withMockedRegistry(
        registry -> {
          List<HttpRequestInterceptor> interceptors = registry.getInterceptors();
          assertFalse(interceptors.isEmpty());
          assertInstanceOf(AuthStrippingInterceptor.class, interceptors.get(0));
        });
  }

  @Test
  void testGetOciTransport_ReturnsNonNull_WhenEndpointProvided() throws Exception {
    withMockedRegistry(registry -> assertNotNull(registry.getOciTransport()));
  }

  @Test
  void testGetOciTransport_ReturnsNull_WhenNoEndpoint() {
    AwsRegistry registry = new AwsRegistry();
    assertNull(registry.getOciTransport());
  }

  @Test
  void testGetAuthorizationHeader_ReturnsValidBasicHeader() throws Exception {
    String expectedToken = "my-ecr-password";
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(
            tokenResponse(
                authDataWithExpiry(
                    expectedToken, Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      String header = registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo");

      assertNotNull(header);
      assertTrue(header.startsWith("Basic "));
      String decoded =
          new String(
              Base64.getDecoder().decode(header.substring("Basic ".length())),
              StandardCharsets.UTF_8);
      assertEquals(AUTH_USERNAME + ":" + expectedToken, decoded);
    }
  }

  @Test
  void testGetAuthorizationHeader_TokenCachedWithinHalfwayWindow_NoRefresh() throws Exception {
    String expectedToken = "cached-token";
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(
            tokenResponse(
                authDataWithExpiry(
                    expectedToken, Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo");
      registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"); // second call should use cache
      verify(mockEcrClient, times(1))
          .getAuthorizationToken(any(GetAuthorizationTokenRequest.class));
    }
  }

  @Test
  void testGetAuthorizationHeader_TokenPastHalfwayPoint_Refreshes() throws Exception {
    String firstToken = "first-token";
    String refreshedToken = "refreshed-token";
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(tokenResponse(authDataWithExpiry(firstToken, Instant.now().minusSeconds(1))))
        .thenReturn(
            tokenResponse(
                authDataWithExpiry(
                    refreshedToken, Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"); // primes cache
      String header =
          registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"); // triggers refresh
      assertEquals(refreshedToken, decodeTokenFromBasicHeader(header));
      verify(mockEcrClient, times(2))
          .getAuthorizationToken(any(GetAuthorizationTokenRequest.class));
    }
  }

  @Test
  void testGetAuthorizationHeader_EmptyAuthorizationData_ThrowsUnknownException() throws Exception {
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(
            GetAuthorizationTokenResponse.builder()
                .authorizationData(Collections.emptyList())
                .build());

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      UnknownException ex =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"));
      assertEquals(ERR_EMPTY_AUTH_DATA, ex.getMessage());
    }
  }

  @Test
  void testGetAuthorizationHeader_InvalidTokenFormat_ThrowsUnknownException() throws Exception {
    EcrClient mockEcrClient = mock(EcrClient.class);
    AuthorizationData authData =
        AuthorizationData.builder()
            .authorizationToken(Base64.getEncoder().encodeToString("invalidtoken".getBytes()))
            .expiresAt(Instant.now().plusSeconds(TOKEN_VALIDITY_SECONDS))
            .build();
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(tokenResponse(authData));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      UnknownException ex =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"));
      assertEquals(ERR_INVALID_TOKEN_FORMAT, ex.getMessage());
    }
  }

  @Test
  void testGetAuthorizationHeader_RefreshFails_FallsBackToCachedToken() throws Exception {
    String cachedToken = "still-valid-token";
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenReturn(tokenResponse(authDataWithExpiry(cachedToken, Instant.now().minusSeconds(1))))
        .thenThrow(awsException(AWS_ERROR_CODE_UNAVAILABLE));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"); // primes cache
      String header =
          registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"); // falls back to cached
      assertEquals(cachedToken, decodeTokenFromBasicHeader(header));
    }
  }

  @Test
  void testGetAuthorizationHeader_RefreshFails_NoCachedToken_ThrowsUnknownException()
      throws Exception {
    EcrClient mockEcrClient = mock(EcrClient.class);
    when(mockEcrClient.getAuthorizationToken(any(GetAuthorizationTokenRequest.class)))
        .thenThrow(awsException(AWS_ERROR_CODE_UNAVAILABLE));

    try (AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient)) {
      UnknownException ex =
          assertThrows(
              UnknownException.class,
              () -> registry.getAuthorizationHeader(BASIC_CHALLENGE, "my-repo"));
      assertEquals(ERR_FAILED_AUTH_TOKEN, ex.getMessage());
    }
  }

  @Test
  void testClose_WithOciAndEcrClient_ClosesAll() throws Exception {
    EcrClient mockEcrClient = mock(EcrClient.class);
    AwsRegistry registry = createRegistryWithMockEcrClient(mockEcrClient);
    registry.close();
    verify(mockEcrClient).close();
  }

  @Test
  void testClose_WithNullEcrClient_NoError() throws Exception {
    AwsRegistry registry = new AwsRegistry();
    registry.close(); // should not throw
  }

  static Stream<Arguments>
      exceptionMappingProvider() { // NOSONAR - needed for @MethodSource resolution
    return Stream.of(
        arguments(new SubstrateSdkException("test"), SubstrateSdkException.class),
        arguments(awsException(AWS_ERROR_CODE_ACCESS_DENIED), UnAuthorizedException.class),
        arguments(awsException(AWS_ERROR_CODE_UNAVAILABLE), UnknownException.class),
        arguments(new IllegalArgumentException("invalid"), InvalidArgumentException.class),
        arguments(new RuntimeException("unknown"), UnknownException.class));
  }

  @ParameterizedTest
  @MethodSource("exceptionMappingProvider")
  void testGetException(Throwable input, Class<? extends SubstrateSdkException> expected)
      throws Exception {
    withMockedRegistry(registry -> assertEquals(expected, registry.getException(input)));
  }
}
